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

/**
 * Get a FileSystem instance for given URL
 * @param url
 * @returns {AzureFileSystem|S3FileSystem|BunnyCDNFileSystem|FileSystem}
 */
import { URL } from "url";
import { AzureFileSystem } from './filesystem/azure';
import { S3FileSystem } from './filesystem/s3';
import { FileSystem } from './filesystem';
import { BunnyCDNFileSystem } from './filesystem/bunnycdn';
import { LocalFileSystem } from './filesystem/local';

export function fileSystemFromURL(url) {
  const parsed = new URL(url);

  let fileSystem;
  switch(parsed.protocol) {
    case 'azure:':
      fileSystem = new AzureFileSystem({
        azureAccount: url.username,
        azureKey: decodeURIComponent(parsed.password),
        azureContainer: parsed.hostname
      });
      break;

    case 's3:':
      fileSystem = new S3FileSystem({ url });
      break;

    case 'bunnycdn:':
      fileSystem = new BunnyCDNFileSystem({ url });
      break;

    default:
      fileSystem = new LocalFileSystem(parsed.pathname);
  }

  return fileSystem;
}