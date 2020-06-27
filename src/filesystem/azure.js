/*
 * VAEM - Asset manager
 * Copyright (C) 2019  Wouter van de Molengraft
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

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

export class AzureFileSystem extends FileSystem {
  /**
   * Construct a new Azure file system
   * @param {String} azureKey
   * @param {String} azureAccount
   * @param {String} azureContainer
   */
  constructor({
    azureKey,
    azureAccount,
    azureContainer
              }) {
    super(null);

    this.azureAccount = azureAccount;
    this.azureContainer = azureContainer;
    this.sharedKeyCredential = new SharedKeyCredential(
      azureAccount,
      azureKey
    );

    const pipeline = StorageURL.newPipeline(this.sharedKeyCredential);

    const serviceUrl = pipeline && new ServiceURL(
      `https://${azureAccount}.blob.core.windows.net`,
      pipeline
    );

    this.containerURL = serviceUrl && ContainerURL.fromServiceURL(
      serviceUrl,
      azureContainer
    );

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
    const blobURL = BlobURL.fromContainerURL(this.containerURL, this.resolvePath(fileName));
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

    let marker = null;

    const result = [];

    do {
      const response = await this.containerURL.listBlobHierarchySegment(
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
    const blobURL = BlobURL.fromContainerURL(this.containerURL, resolved);
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

    const blockBlokURL = BlockBlobURL.fromContainerURL(this.containerURL, clientPath);

    uploadStreamToBlockBlob(
      Aborter.none,
      stream,
      blockBlokURL,
      4 * 1024 * 1024,
      16
    )
    .then(() => {
      stream.end()
      stream.emit('done');
    })
    .catch(e => {
      console.error(e);
      stream.emit('error', e);
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
    const blockBlokURL = BlockBlobURL.fromContainerURL(this.containerURL, clientPath);
    await blockBlokURL.delete(Aborter.none)
  }

  async rename(from, to) {
    const resolvedFrom = this.resolvePath(from);
    const resolvedTo = this.resolvePath(to);
    console.log(`Renaming ${resolvedFrom} to ${resolvedTo}`);

    const destinationBlobURL = BlobURL.fromContainerURL(this.containerURL, resolvedTo);

    const sourceBlobURL = BlobURL.fromContainerURL(this.containerURL, resolvedFrom);
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
    const containerName = this.azureContainer;
    const result = generateBlobSASQueryParameters({
      containerName,
      blobName,
      permissions: permissions.toString(),
      expiryTime: moment().add(8, 'hours').toDate(),
      protocol: 'https'
    }, this.sharedKeyCredential);

    return `https://${this.azureAccount}.blob.core.windows.net/${containerName}/${blobName}?${result.toString()}`
  }
}
