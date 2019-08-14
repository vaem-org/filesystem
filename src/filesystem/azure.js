import fs, { constants } from 'fs'
import { PassThrough } from 'stream'
import { FileSystem } from '../filesystem'
import { resolve, basename } from 'path'

import {
  Aborter, BlobSASPermissions,
  BlobURL,
  BlockBlobURL,
  ContainerURL,
  generateBlobSASQueryParameters,
  ServiceURL,
  SharedKeyCredential,
  StorageURL,
  uploadStreamToBlockBlob
} from '@azure/storage-blob'

import moment from 'moment'

const sharedKeyCredential = process.env.AZURE_ACCOUNT && new SharedKeyCredential(process.env.AZURE_ACCOUNT,
  process.env.AZURE_KEY);

const pipeline = sharedKeyCredential && StorageURL.newPipeline(sharedKeyCredential);

const serviceUrl = pipeline && new ServiceURL(
  `https://${process.env.AZURE_ACCOUNT}.blob.core.windows.net`,
  pipeline
);

const containerURL = serviceUrl && ContainerURL.fromServiceURL(
  serviceUrl,
  process.env.AZURE_CONTAINER
);

export class AzureFileSystem extends FileSystem {
  constructor() {
    super(null);
    this.workingDirectory = '/'
  }

  currentDirectory() {
    return this.workingDirectory
  }

  resolvePath(path) {
    return resolve(this.workingDirectory, path).substr(1)
  }

  /**
   * Get a Stats object for blob or folder
   * @param {String} name
   * @param {BlobProperties} properties
   * @returns {Promise<module:fs.Stats | Stats>}
   */
  static async getStats(name, properties = null) {
    const stat = new fs.Stats();
    stat.name = basename(name);
    stat.mode = (!properties ? constants.S_IFDIR : constants.S_IFREG) |
      constants.S_IRUSR |
      constants.S_IRGRP |
      constants.S_IROTH;

    stat.atime = new Date();
    stat.ctime = new Date();
    stat.birthtime = properties ? properties.creationTime : new Date();
    stat.mtime = properties ? properties.lastModified : new Date();
    stat.size = properties ? properties.contentLength : 0;
    return stat
  }

  async get(fileName) {
    const blobURL = BlobURL.fromContainerURL(containerURL, this.resolvePath(fileName));
    let properties;

    try {
      properties = await blobURL.getProperties(
        Aborter.none
      )
    }
    catch (e) {
      // TODO: handle directories
    }

    return AzureFileSystem.getStats(fileName, properties)
  }

  async list(path) {
    const resolved = this.resolvePath(path);

    console.log(`Listing ${resolved}`);
    let marker = null;

    const result = [];

    do {
      const response = await containerURL.listBlobHierarchySegment(
        Aborter.none,
        '/',
        marker,
        resolved ? {
          prefix: `${resolved}/`
        } : {}
      );

      for (let prefix of response.segment.blobPrefixes) {
        result.push(await AzureFileSystem.getStats(prefix.name.slice(0, -1)))
      }

      for (let item of response.segment.blobItems) {
        result.push(await AzureFileSystem.getStats(item.name, item.properties))
      }

      marker = response.marker
    }
    while (marker);

    return result
  }

  async chdir(path) {
    this.workingDirectory = path;
    return path
  }

  async read(filename, { start = undefined } = {}) {
    const resolved = this.resolvePath(filename);
    console.log(`Reading ${resolved}`);
    const blobURL = BlobURL.fromContainerURL(containerURL, resolved);
    const response = await blobURL.download(
      Aborter.none,
      start || 0
    );

    return {
      stream: response.readableStreamBody,
      clientPath: resolved
    }
  }

  async write(filename, { append = false, start = undefined } = {}) {
    const clientPath = this.resolvePath(filename);
    const stream = new PassThrough();

    const blockBlokURL = BlockBlobURL.fromContainerURL(containerURL, clientPath);

    uploadStreamToBlockBlob(
      Aborter.none,
      stream,
      blockBlokURL,
      4 * 1024 * 1024,
      16
    )
    .then(() => {
      stream.end()
    })
    .catch(e => {
      console.error(e);
      stream.end()
    });

    return {
      stream,
      clientPath
    }
  }

  async delete(path) {
    const clientPath = this.resolvePath(path);
    console.log(`Deleting ${clientPath}`);
    const blockBlokURL = BlockBlobURL.fromContainerURL(containerURL, clientPath);
    await blockBlokURL.delete(Aborter.none)
  }

  async rename(from, to) {
    const resolvedFrom = this.resolvePath(from);
    const resolvedTo = this.resolvePath(to);
    console.log(`Renaming ${resolvedFrom} to ${resolvedTo}`);

    const destinationBlobURL = BlobURL.fromContainerURL(containerURL, resolvedTo);

    const sourceBlobURL = BlobURL.fromContainerURL(containerURL, resolvedFrom);
    await destinationBlobURL.startCopyFromURL(
      Aborter.none,
      sourceBlobURL.url
    );

    await sourceBlobURL.delete(
      Aborter.none
    )
  }

  async getSignedUrl(filename) {
    const permissions = new BlobSASPermissions();
    permissions.read = true;
    const blobName = this.resolvePath(filename);
    const containerName = process.env.AZURE_CONTAINER;
    const result = generateBlobSASQueryParameters({
      containerName,
      blobName,
      permissions: permissions.toString(),
      expiryTime: moment().add(1, 'hours').toDate(),
      protocol: 'https'
    }, sharedKeyCredential);

    return `https://${process.env.AZURE_ACCOUNT}.blob.core.windows.net/${containerName}/${blobName}?${result.toString()}`
  }
}
