"use strict";

const fs = require("fs");
const path = require("path");

const { promisify } = require("util");

const Parameters = require("../parameters").get();

const readdirAsync = promisify(fs.readdir);
const readFileAsync = promisify(fs.readFile);

/**
 *
 * @param {Array<{event: "TransferSingle" | "TransferBatch", returnValues: string[]}>} pastEvents
 * @returns
 */
const getMinimal = (pastEvents) =>
  pastEvents.reduce((acc, event) => {
    if (event["event"] === "TransferSingle") {
      acc = acc.concat(
        Array(Number(event.returnValues["4"])).fill({ transactionHash: event.transactionHash, from: event.returnValues["1"], to: event.returnValues["2"], tokenId: event.returnValues["3"] })
      );
    } else if (event["event"] === "TransferBatch") {
      for (let ii = 0, ll = event.returnValues["4"].length; ii < ll; ii++) {
        acc = acc.concat(
          Array(Number(event.returnValues["4"][ii])).fill({ transactionHash: event.transactionHash, from: event.returnValues["1"], to: event.returnValues["2"], tokenId: event.returnValues["3"][ii] })
        );
      }
    }
    return acc;
  }, []);

module.exports.getEvents = async (symbol) => {
  const directory = Parameters.eventsDownloadFolder.replace(/{token}/g, symbol);
  const directoryCombined = Parameters.eventsDownloadFolder.replace(/{token}/g, symbol + "_Combined");
  var files = await readdirAsync(directory);
  files.sort((a, b) => {
    return parseInt(a.split(".")[0]) - parseInt(b.split(".")[0]);
  });
  let events = [];

  console.log("Combining files...");

  //Make sure combined folder exists
  if(!fs.existsSync(directoryCombined)) {
    fs.mkdirSync(directoryCombined);
  }

  let combineFileName = path.join(directoryCombined, "combinedFull.json");
  let combineFileNameTemp = path.join(directoryCombined, "combinedFull_tmp.json");
  let lastCombinedFileName = path.join(directoryCombined, "lastCombined.txt");
  let lastCombinedBlock = 0;

  if(fs.existsSync(lastCombinedFileName)) {
    const contents = await readFileAsync(lastCombinedFileName);
    lastCombinedBlock = Number(contents.toString());
  }
  
  //Clear out temp file if it exists (interupted last processing)
  if(fs.existsSync(combineFileNameTemp)) {
    fs.unlinkSync(combineFileNameTemp);
  }
  
  //If combineFileName existst, copy it over to temp
  let isFirstFile = true;
  if(fs.existsSync(combineFileName)) {
    fs.copyFileSync(combineFileName, combineFileNameTemp);
    isFirstFile = false;
  }

  var stream = fs.createWriteStream(combineFileNameTemp, {flags:'a'});


  //Load up all the files, and combine them into one large file
  if(lastCombinedBlock > 0 && Number(files[files.length - 1].toString().slice(0, -5) > lastCombinedBlock)) {
    for await (const file of files) {
      let blockNumber = Number(file.toString().slice(0, -5));
      if(blockNumber <= lastCombinedBlock) {
        //Already combined this file, continue
        continue;
      }

      // console.log("combining additional file " + file);
      
      const contents = await readFileAsync(path.join(directory, file));
      const parsed = JSON.parse(contents.toString());
      const minimal = getMinimal(parsed);
      if(minimal && minimal.length > 0) {
        let minimalJSON = JSON.stringify(minimal);

        //Remove last char which would be the closing array ']'
        minimalJSON = minimalJSON.slice(0, -1);

        //Remove first char which would be the opening array ']'
        minimalJSON = minimalJSON.slice(1);

        if(!isFirstFile) {
          //Add comma and new line from previous file
          minimalJSON = ",\n" + minimalJSON;
        } else {
          isFirstFile = false;
        }
        
        stream.write(minimalJSON);
      } else {
        //File is empty, skipping
      }
    }
  }
  
  stream.end();

  //Copy over from temp to final
  if(fs.existsSync(combineFileName)) {
    fs.unlinkSync(combineFileName);
  }

  fs.renameSync(combineFileNameTemp, combineFileName);

  //write last combined block
  await fs.writeFileSync(lastCombinedFileName, files[files.length - 1].toString().slice(0, -5));

  //Loading up one big file
  console.log("Loading up combined events file...");
  const contents = await readFileAsync(combineFileName);
  const contentsString = contents.toString();
  const parsed = JSON.parse("[" + contentsString + "]");
  
  return parsed;
};
