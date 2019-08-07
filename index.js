const couchbase = require("couchbase");

/*****
    SELECT * FROM `bucket`
    WHERE META().id LIKE CropRotationTemplate::lmod%
    WHERE META().id LIKE CropRotationTemplate::agc%
    OR META().id LIKE CropRotationTemplate::dcid%

    SELECT * FROM `bucket`
    WHERE META().id LIKE LMODCropRotationTemplateOperation::lmodid%
*****/
async function importTemplates(bucket) {
  let templates = require("./files/cr-lmod-managements_6-11-18.json");

  templates = templates.map(t => ({
    key: `CropRotationsTemplate::lmod::${t.man_id}`,
    data: {
      id: t.man_id,
      name: t.man_name,
      path: t.man_path,
      cmz: t.man_cmz,
      stir: t.man_stir,
      duration: t.man_duration,
      documentType: "LMODCropRotationsTemplate"
    }
  }));

  for (t in templates) {
    await insert(bucket, templates[t].key, templates[t].data);
  }

  return "Crop Rotations Templates Imported Successfully.";
}

async function importCrops(bucket) {
  let crops = require("./files/cr-lmod-crops_6-11-18.json");

  crops = {
    key: `CropRotationsCrops`,
    data: crops.map(c => ({
      id: c.crop_id,
      name: c.crop_name,
      yield: c.crop_yield,
      unit: c.crop_yield_unit
    }))
  };

  await insert(bucket, crops.key, crops.data);

  return "Crop Rotations Crops Imported Successfully.";
}

async function importResidus(bucket) {
  let residus = require("./files/cr-lmod-residues_6-11-18.json");

  residus = {
    key: `CropRotationsResidus`,
    data: residus.map(r => ({
      id: r.res_id,
      name: r.res_name
    }))
  };

  await insert(bucket, residus.key, residus.data);

  return "Crop Rotations Residus Imported Successfully.";
}

async function importOperations(bucket) {
  let operations = require("./files/cr-lmod-operations_6-11-18.json");

  operations = {
    key: `CropRotationOperations`,
    data: operations.map(o => ({
      id: o.op_id,
      name: o.op_name,
      group: o.op_group1,
      stir: o.op_stir,
      growth: o.op_begin_growth,
      killCrop: o.op_kill_crop,
      addResidue: o.op_add_residue,
      resAdded: o.op_res_added
    }))
  };

  await insert(bucket, operations.key, operations.data);

  return "Crop Rotations Operations Imported Successfully.";
}

async function importCropRotationsTemplateOperations() {
  let operations = require("./files/cr-lmod-operations_6-11-18.json");
  let events = require("./files/cr-lmod-events_6-11-18.json");
  let crops = require("./files/cr-lmod-crops_6-11-18.json");
  let residus = require("./files/cr-lmod-residues_6-11-18.json");

  events = events.map(e => ({
    key: `LMODCropRotationsTemplateOperation::${e.fk_man_id}::${e.fk_op_id}`,
    data: {
      date: e.date,
      operation: operations
        .filter(o => o.op_id == e.fk_op_id)
        .map(o => ({
          id: o.op_id,
          name: o.op_name,
          group: o.op_group1,
          stir: o.op_stir,
          growth: o.op_begin_growth,
          killCrop: o.op_kill_crop,
          addResidue: o.op_add_residue,
          resAdded: o.op_res_added
        }))[0],
      crop: e.fk_crop_id
        ? crops
            .filter(c => c.crop_id == e.fk_crop_id)
            .map(c => ({
              id: c.crop_id,
              name: c.crop_name,
              yield: c.crop_yield,
              unit: c.crop_yield_unit
            }))[0]
        : null,
      residu: e.fk_res_id
        ? residus
            .filter(r => r.res_id == e.fk_res_id)
            .map(r => ({ id: r.res_id, name: r.res_name }))[0]
        : null,
      residuAmount: e.res_amount,
      targetYield: e.target_yield
    }
  }));

  for (e in events) await insert(bucket, events[e].key, events[e].data);

  return "Crop Rotations Templates Operations Successfully.";
}

function connectDB(url, username, password, bucketName) {
  const cluster = new couchbase.Cluster(url);
  cluster.authenticate(username, password);

  return cluster.openBucket(bucketName);
}

async function insert(bucket, key, data) {
  return new Promise((resolve, reject) => {
    bucket.upsert(key, data, (err, result) => {
      if (err) {
        console.log(err);
        reject(err);
      } else {
        console.log(`Insert into Couchbase successfully. Key (${key}).`);
        resolve(result);
      }
    });
  });
}

const bucket = connectDB(
  "couchbase://localhost:8091/",
  "Administrator",
  "123456",
  "Customer360"
);

importTemplates(bucket)
  .then(r => console.log(r))
  .catch(err => console.log(err));

importCrops(bucket)
  .then(r => console.log(r))
  .catch(err => console.log(err));

importResidus(bucket)
  .then(r => console.log(r))
  .catch(err => console.log(err));

importOperations(bucket)
  .then(r => console.log(r))
  .catch(err => console.log(err));

importCropRotationsTemplateOperations(bucket)
  .then(r => console.log(r))
  .catch(err => console.log(err));

/* DELETE FROM `Customer360` */
/* SELECT * FROM `Customer360` AS data WHERE META().id = 'CropRotationsCrops' */
/* SELECT * FROM `Customer360` AS data WHERE META().id = 'CropRotationsResidus' */
/* SELECT * FROM `Customer360` AS data WHERE META().id LIKE 'CropRotationsTemplate::lmod%' */
