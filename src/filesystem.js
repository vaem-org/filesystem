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

import { FileSystem as FTPSrvFileSystem } from 'ftp-srv'
import { join } from 'path'
import { Readable } from 'stream'

export class FileSystem extends FTPSrvFileSystem {
  async getSignedUrl(filename) {
    return false
  }

  async ensureDir(dirname) {

  }

  async recursivelyDelete(dirname) {
    const files = await this.list(dirname);

    for(let file of files) {
      if (file.isDirectory()) {
        await this.recursivelyDelete(join(dirname, file.name));
      } else {
        await this.delete(join(dirname, file.name));
      }
    }
  }

  async writeFile(fileName, content) {
    const { stream } = await this.write(fileName);

    const source = new Readable();
    source.pipe(stream);
    source.push(content);
    source.push(null);
  }

  async readFile(fileName) {
    const { stream } = await this.read(fileName);
    const buffers = [];

    await (new Promise((accept, reject) => {
      stream.on('data', buf => buffers.push(buf));
      stream.on('end', accept);
      stream.on('error', reject);
    }));

    return Buffer.concat(buffers);
  }
}