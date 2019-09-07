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

import { URL } from 'url';
import fs, { constants } from 'fs';
import S3 from 'aws-sdk/clients/s3';
import { basename, resolve } from 'path';

import { FileSystem } from '../filesystem';
import { PassThrough } from "stream";

export class S3FileSystem extends FileSystem {
  constructor({ url }) {
    super(null);

    const parsed = new URL(url);

    this.s3 = new S3({
      endpoint: parsed.hostname,
      params: {
        Bucket: parsed.pathname.substr(1)
      },
      accessKeyId: parsed.username,
      secretAccessKey: decodeURIComponent(parsed.password)
    });

    this.workingDirectory = '/';
  }

  resolvePath(path) {
    return resolve(this.workingDirectory || '/', path).substr(1)
  }

  currentDirectory() {
    return this.workingDirectory;
  }

  /**
   * Get a Stats object for blob or folder
   * @param {String} name
   * @param {{}} properties
   * @returns {Promise<module:fs.Stats | Stats>}
   */
  static getStats(name, properties = null) {
    const stat = new fs.Stats();
    stat.name = basename(name);
    stat.mode = (!properties ? constants.S_IFDIR : constants.S_IFREG) |
      constants.S_IRUSR |
      constants.S_IRGRP |
      constants.S_IROTH;

    stat.atime = new Date();
    stat.ctime = new Date();
    stat.birthtime = properties ? properties.LastModified : new Date();
    stat.mtime = properties ? properties.LastModified : new Date();
    stat.size = properties ? properties.Size || properties.ContentLength : 0;
    return stat
  }

  async get(fileName) {
    let response = null;
    try {
      const Key = this.resolvePath(fileName);
      response = Key ? await this.s3.headObject({
        Key
      }).promise() : null;
    }
    catch (e) {

    }

    return S3FileSystem.getStats(fileName, response);
  }

  async list(path) {
    const resolved = this.resolvePath(path);

    let result = [];
    let isTruncated;
    let ContinuationToken = null;

    do {
      const response = await this.s3.listObjectsV2({
        Prefix: resolved ? `${resolved}/` : '',
        Delimiter: '/',
        ContinuationToken
      }).promise();

      result = [
        ...result,
        ...response.CommonPrefixes.map(
          ({ Prefix }) => S3FileSystem.getStats(basename(Prefix))
        ),
        ...response.Contents.map(
          properties => S3FileSystem.getStats(basename(properties.Key), properties)
        )
      ];

      isTruncated = response.IsTruncated;
      ContinuationToken = response.ContinuationToken;
    }
    while (isTruncated);

    return result;
  }

  async chdir(path) {
    this.workingDirectory = '/' + this.resolvePath(path);
    return this.workingDirectory
  }

  async read(filename, { start = undefined } = {}) {
    const clientPath = this.resolvePath(filename);
    const stream = this.s3.getObject({
      Key: clientPath,
      Range: start ? `bytes=${start}-` : null
    }).createReadStream();

    return {
      stream,
      clientPath
    };
  }

  async write(fileName, { append = false, start = undefined } = {}) {
    const clientPath = this.resolvePath(fileName);
    const stream = new PassThrough();

    if (append || start) {
      throw 'Appending to S3 is unsupported';
    }

    this.s3.upload({
      Key: clientPath,
      Body: stream
    }).promise()
    .catch(e => {
      console.error(`An error occurred uploading file '${fileName}' to S3: ${e.toString()}`);
      stream.emit('error', e);
    });

    return {
      stream,
      clientPath
    }
  }

  async delete(path) {
    return this.s3.deleteObject({
      Key: this.resolvePath(path)
    }).promise();
  }

  async rename(from, to) {
    await this.s3.copyObject({
      Key: this.resolvePath(to),
      CopySource: this.resolvePath(from)
    }).promise();

    await this.s3.deleteObject({
      Key: this.resolvePath(from)
    })
  }

  async ensureDir() {
    // directories do not exist
  }
}
