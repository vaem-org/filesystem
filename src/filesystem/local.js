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

import { FileSystem } from '../filesystem';
import { resolve } from 'path';
import { ensureDir } from 'fs-extra';

export class LocalFileSystem extends FileSystem {
  constructor(root) {
    super(null, {root, cwd: '/'});

    this.root = root;
  }

  async ensureDir(dirname) {
    if (dirname.startsWith('.')) {
      throw 'No relative paths allowed';
    }

    const resolved = resolve(this.root, dirname);
    await ensureDir(resolved);
  }
}