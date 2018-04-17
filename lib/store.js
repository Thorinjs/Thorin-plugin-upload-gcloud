'use strict';
const Storage = require('@google-cloud/storage');
const url = require('url'),
  fs = require('fs'),
  path = require('path'),
  concat = require('concat-stream');


const GCLOUD_STORAGE_URL = 'https://storage.googleapis.com/',
  GCLOUD_STORAGE_HOST = 'storage.googleapis.com';

/*
 * This is the abstraction over the Google Cloud Storage file uploader.
 * */
module.exports = function (thorin, opt, IStorage) {

  opt = thorin.util.extend({
    logger: opt.logger || 'upload-gcloud'
  }, opt);

  const config = Symbol(),
    sdk = Symbol(),
    logger = thorin.logger(opt.logger);

  class GcloudStorage extends IStorage {
    /*
     * The Google Cloud Storage requires the following options:
     *   - bucket -> the bucket to use.
     *   - credentials -> the Google Cloud credentials to use
     * */
    constructor(options, name) {
      super(name || 'gcloud');
      this[config] = thorin.util.extend({
        bucket: null,
        credentials: null
      }, options);
      if (process.env.GOOGLE_APPLICATION_CREDENTIALS && (!this[config].credentials || (typeof this[config].credentials === 'object' && Object.keys(this[config].credentials).length === 0))) {
        this[config].credentials = process.env.GOOGLE_APPLICATION_CREDENTIALS;
      }
      if (typeof this[config].credentials === 'string' && this[config].credentials) {
        this[config].credentials = this[config].credentials.trim();
        if (this[config].credentials.charAt(0) === '{') {
          try {
            this[config].credentials = JSON.parse(this[config].credentials);
          } catch (e) {
            throw thorin.error('STORE.GCLOUD', 'Credentials could not be parsed');
          }
        } else {
          let credPath = this[config].credentials.charAt(0) === '/' ? path.normalize(this[config].credentials) : path.normalize(thorin.root + '/' + this[config].credentials);
          try {
            let creds = fs.readFileSync(credPath, {encoding: 'utf8'});
            creds = JSON.parse(creds);
            this[config].credentials = creds;
          } catch (e) {
            throw thorin.error('STORE.GCLOUD', 'Credentials could not be read [' + credPath + ']');
          }
        }
      }
      if (!this[config].credentials) {
        logger.warn(`GCloud Storage: missing credentials`);
        return;
      }
      if (!this[config].bucket) {
        logger.warn('GCloud Storage: missing bucket in configuration');
        return;
      }
      this[sdk] = new Storage({
        credentials: this[config].credentials
      });
    }

    get bucket() {
      return this[config].bucket || null;
    }

    /*
     * Store the given file to Google Cloud.
     * */
    save(fileObj) {
      return new Promise((resolve, reject) => {
        const bucketObj = this[sdk].bucket(this[config].bucket);
        let sFileObj = new Storage.File(bucketObj, fileObj.getKey(), {
          resumable: false
        });
        let fOpt = thorin.util.extend({
          contentType: fileObj.mimeType,
          ACL: 'private'
        }, fileObj.options);
        if (fOpt.ACL) {
          fOpt.predefinedAcl = fOpt.ACL;
          delete fOpt.ACL;
        }
        if (fOpt.predefinedAcl === 'public-read') fOpt.predefinedAcl = 'publicRead';
        let streamObj = fileObj.getStream();

        function onDone(err, res) {
          if (err) {
            logger.warn('Failed to finalize upload of file ' + fileObj.getKey());
            logger.debug(err);
            return reject(thorin.error('UPLOAD.STORAGE_FAILED', 'Could not finalize the file upload', err));
          }
          let finalUrl = GCLOUD_STORAGE_URL + sFileObj.bucket.name + '/' + sFileObj.name;
          // IF the file contains any kind of errors, we remove it.
          if (!fileObj.error) {
            fileObj.url = finalUrl;
            return resolve();
          }
          logger.trace(`Removing failed uploaded image ${finalUrl}`);
          resolve();
          this.remove(finalUrl).catch((e) => {
            if (e) {
              logger.error(`Failed to remove failed uploaded image ${finalUrl}`);
              logger.debug(e);
            }
          });
        }

        if (typeof streamObj === 'string') {
          sFileObj.save(streamObj, fOpt, onDone);
        } else {
          let remoteWrite = sFileObj.createWriteStream({
            metadata: {
              contentType: fOpt.contentType
            }
          });
          streamObj.pipe(remoteWrite)
            .on('error', onDone)
            .on('finish', onDone)
        }
      });

    }

    /**
     * Download a file from the given URL
     * OPTIONS
     * opt.raw -> if set to true, we will resolve with the readable stream.
     * */
    download(fileUrl, opt) {
      if (typeof fileUrl === 'object' && fileUrl && typeof fileUrl.url === 'string') {
        fileUrl = fileUrl.url;
      }
      if (!opt) opt = {};
      if (typeof fileUrl !== 'string' || !fileUrl) {
        return Promise.reject(thorin.error('UPLOAD.DOWNLOAD_FAILED', 'Download URL is not valid'));
      }
      let fopt = {};
      if (fileUrl.indexOf('://') !== -1) {
        fopt = getUrlOptions(fileUrl);
        if (!fopt.key) {
          return Promise.reject(thorin.error('UPLOAD.DOWNLOAD_FAILED', 'Download URL is not valid'));
        }
      } else {
        fopt.key = fileUrl;
      }
      if (!fopt.bucket) fopt.bucket = this[config].bucket;
      return new Promise((resolve, reject) => {
        const bucketObj = this[sdk].bucket(fopt.bucket);
        let sFileObj = new Storage.File(bucketObj, fopt.key);
        let readStr = sFileObj.createReadStream(opt);
        if (opt.raw === true) {
          return resolve(readStr);
        }
        let contentType, isDone = false;

        function onDone(e) {
          if (isDone) return;
          isDone = true;
          if (e.code !== 404) {
            logger.warn(`Failed to download file ${fopt.key}`);
            logger.debug(e);
          }
          let err = thorin.error('UPLOAD.DOWNLOAD_FAILED', 'Could not download file', e);
          err.statusCode = e.code;
          reject(err);
        }

        function onData(d) {
          if (isDone) return;
          isDone = true;
          if (!contentType) return resolve(d);
          let isString = false;
          if (contentType.indexOf('text/') !== -1) {
            isString = true;
          } else if (contentType.indexOf('json') !== -1) {
            isString = true;
          } else if (contentType.indexOf('javascript') !== -1) {
            isString = true;
          }
          if (isString) d = d.toString();
          resolve(d);
        }

        function onRes(res) {
          if (res.headers['content-type']) {
            contentType = res.headers['content-type'];
          }
        }

        readStr
          .on('error', onDone)
          .on('response', onRes)
          .pipe(concat(onData));
      });
    }

    /**
     * Generates a signed URL to download a file
     * Arguments:
     * - opt.bucket - the bucket name
     * - opt.key - the key name
     * OR
     * - opt - a full URL of the object, we extract the bucket/key from there.
     * RETURNS  A PROMISE RESOLVING WITH THE ACTUAL SIGNED URL.
     * */
    getSignedUrl(opt, expire) {
      return new Promise((resolve, reject) => {
        opt = getUrlOptions(opt);
        if (!opt.bucket) opt.bucket = this[config].bucket;
        if (!opt.key) return resolve(null);
        if (typeof expire === 'number') opt.expire = expire;
        if (typeof opt.expire === 'undefined') {
          opt.expire = 60;
        }
        const bucketObj = this[sdk].bucket(opt.bucket);
        const fileObj = new Storage.File(bucketObj, opt.key);
        const fOpt = {
          action: opt.action || 'read'
        };
        if (opt.expire) {
          let expireAt = new Date(Date.now() + opt.expire * 1000);
          fOpt.expires = expireAt.toISOString();
        }
        fileObj.getSignedUrl(fOpt, (err, url) => {
          if (err) {
            logger.debug(`Could not generate signed URL for: ${params.Key}-${params.Bucket}`);
            logger.debug(err);
            return resolve(null);
          }
          resolve(url);
        });
      });
    }

    /**
     * Checks if we can remove the given URL from aws.
     * */
    canRemove(fileUrl) {
      let opt = getUrlOptions(fileUrl);
      if (!opt.bucket) return false;
      return true;
    }

    /**
     * Removes the given URL from storage
     * Opt - a full URL or a {key,bucket} object
     * */
    remove(opt) {
      return new Promise((resolve, reject) => {
        opt = getUrlOptions(opt);
        if (!opt.bucket) opt.bucket = this[config].bucket;
        if (!opt.key) return resolve(null);
        const bucketObj = this[sdk].bucket(opt.bucket);
        const fileObj = new Storage.File(bucketObj, opt.key);
        fileObj.delete((err) => {
          if (err) {
            if (err.code === 404) {
              logger.debug(`File ${opt.key} not found for delete`);
              return resolve(false);
            }
            logger.trace('Failed to remove storage key ' + opt.key);
            return reject(err);
          }
          resolve(true);
        });
      });
    }

    /* Destroys the instance and all its properties, cleaning up. */
    destroy() {
      delete this[config];
      delete this[sdk];
    }

    getSdk() {
      return this[sdk];
    }

  }

  function getUrlOptions(opt) {
    if (typeof opt === 'string' && opt) {
      try {
        let d = url.parse(opt);
        opt = {};
        if (!d) throw 1;
        if (d.hostname === GCLOUD_STORAGE_HOST) {
          let tmp = d.pathname.split('/');
          if (tmp[0] === '') tmp.splice(0, 1);
          let bucketName = tmp.splice(0, 1)[0],
            key = '/' + tmp.join('/');
          opt.bucket = bucketName;
          opt.key = key;
        } else {
          throw 2;
        }
      } catch (e) {
        return null;
      }
    } else if (!opt) {
      opt = {};
    }
    return opt;
  }

  return GcloudStorage;
}
