import { FileSystem as FTPSrvFileSystem } from 'ftp-srv'

export class FileSystem extends FTPSrvFileSystem {
  async getSignedUrl(filename) {
    return false
  }
}