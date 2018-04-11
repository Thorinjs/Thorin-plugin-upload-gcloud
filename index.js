'use strict';

/**
 * This is the Thorin Google Cloud storage for file uploads.
 * */
const initStore = require('./lib/store');
module.exports = function (thorin, opt, pluginName) {
  opt = thorin.util.extend({
    name: 'gcloud',
    uploader: 'upload'
  }, opt);
  let GcloudStorage = null;
  thorin.on(thorin.EVENT.INIT, 'plugin.' + opt.uploader, (pluginObj) => {
    GcloudStorage = initStore(thorin, opt, pluginObj.IStorage);
    pluginObj.registerStorageClass('gcloud', GcloudStorage);
    /*
     * Manually create a storage instance.
     * */
    pluginObj.create = function CreateInstance(name, opt) {
      return new GcloudStorage(opt, name);
    }
  });

  const pluginObj = {};
  pluginObj.getStorage = function () {
    return GcloudStorage;
  };
  pluginObj.options = opt;

  return pluginObj;
};
module.exports.publicName = 'upload-gcloud';