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

import _axios from 'axios';
import { URL } from 'url';
import { PassThrough } from 'stream';
import { FileSystem } from '../filesystem';
import { basename, resolve, dirname } from 'path';
import fs, { constants } from 'fs';
import moment from 'moment';
import NodeCache from 'node-cache';

export class BunnyCDNFileSystem extends FileSystem {
  constructor({ url }) {
    super(null);

    const parsed = new URL(url);
    this.workingDirectory = '/';
    this.axios = _axios.create({
      baseURL: `https://storage.bunnycdn.com/${parsed.hostname}/`,
      headers: {
        'AccessKey': parsed.username
      },
      maxContentLength: Infinity,
      maxBodyLength: Infinity
    });

    this.listCache = new NodeCache({stdTTL: 60});
  }

  resolvePath(path) {
    return resolve(this.workingDirectory || '/', path).substr(1);
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
    stat.mode = (!properties || properties.IsDirectory ? constants.S_IFDIR : constants.S_IFREG) |
      constants.S_IRUSR |
      constants.S_IRGRP |
      constants.S_IROTH;

    stat.atime = new Date();
    stat.ctime = new Date();
    stat.birthtime = properties ? moment(properties.DateCreated).toDate() : new Date();
    stat.mtime = properties ? moment(properties.LastChanged).toDate() : new Date();
    stat.size = properties ? properties.Length : 0;
    return stat
  }

  async get(fileName) {
    // maybe not the most efficient, but there is no other way
    const dir = dirname(fileName);
    const list = this.listCache.get(dir) || await this.list(dir);

    const base = basename(fileName);
    return list.find(entry => entry.name === base);
  }

  async list(path) {
    const response = await this.axios.get(`${this.resolvePath(path)}/`);
    const result = response.data.map(entry => {
      return BunnyCDNFileSystem.getStats(entry.ObjectName, entry)
    });

    this.listCache.set(path, result);
    return result;
  }

  async chdir(path) {
    this.workingDirectory = '/' + this.resolvePath(path);
    return this.workingDirectory
  }

  async read(filename, {start=undefined}={}) {
    const clientPath = this.resolvePath(filename);

    const stream = (await this.axios.get(clientPath, {
      responseType: 'stream',
      headers: start ? {
        'Range': `bytes=${start}-`
      } : null
    })).data;

    return {
      stream,
      clientPath
    }
  }

  async write(filename, {append=false, start=undefined}={}) {
    const clientPath = this.resolvePath(filename);
    const stream = new PassThrough();

    if (append || start) {
      throw 'Append not supported';
    }

    this.axios.put(
      clientPath,
      stream
    ).catch(err => {
      console.error(`Error uploading file ${filename} to BunnyCDN: ${err.toString()}`);
      stream.emit('error', err);
    }).then(() => {
      stream.emit('done');
    });

    return {
      clientPath,
      stream
    }
  }

  async delete(filename) {
    const resolved = this.resolvePath(filename);
    await this.axios.delete(resolved);
  }

  async recursivelyDelete(dirname) {
    return this.axios.delete(this.resolvePath(dirname) + '/');
  }
}
