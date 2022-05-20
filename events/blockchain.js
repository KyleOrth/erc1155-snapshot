"use strict";

const Web3 = require("web3");

const BlockReader = require("./block-reader");
const Config = require("../config").getConfig();
const Contract = require("../contract").getContract();
const FileHelper = require("../file-helper");
const LastDownloadedBlock = require("./last-downloaded-block");
const Parameters = require("../parameters").get();

const { promisify } = require("util");

const sleep = promisify(setTimeout);

const web3 = new Web3(new Web3.providers.HttpProvider((Config || {}).provider || "http://localhost:8545"));

const groupBy = (objectArray, property) => {
  return objectArray.reduce((acc, obj) => {
    var key = obj[property];
    if (!acc[key]) {
      acc[key] = [];
    }
    acc[key].push(obj);
    return acc;
  }, {});
};

const tryGetEvents = async (start, end, symbol) => {
  for (;;) {
    try {
      const pastEventsSingle = await Contract.getPastEvents("TransferSingle", { fromBlock: start, toBlock: end });
      const pastEventsBatch = await Contract.getPastEvents("TransferBatch", { fromBlock: start, toBlock: end });

      if (pastEventsSingle.length) {
        console.info("Successfully imported Single", pastEventsSingle.length, " events");
      }
      if (pastEventsBatch.length) {
        console.info("Successfully imported Batch", pastEventsBatch.length, " events");
      }

      const groupSingle = groupBy(pastEventsSingle, "blockNumber");
      const groupBatch = groupBy(pastEventsBatch, "blockNumber");

      const groupSingleKeys = Object.keys(groupSingle);
      const groupBatchKeys = Object.keys(groupBatch);

      const finalKeys = Array.from(new Set(groupSingleKeys.concat(groupBatchKeys)));

      const group = {};

      finalKeys.forEach((v) => {
        /**@type Array<{logIndex: number}> */
        const single = groupSingle[v] || [];
        /**@type Array<{logIndex: number}> */
        const batch = groupBatch[v] || [];

        group[v] = single.concat(batch).sort((a, b) => a.logIndex - b.logIndex);
      });

      for (let key in group) {
        if (group.hasOwnProperty(key)) {
          const blockNumber = key;
          const data = group[key];

          const file = Parameters.eventsDownloadFilePath.replace(/{token}/g, symbol).replace(/{blockNumber}/g, blockNumber);

          await FileHelper.writeFile(file, data);
        }
      }
      break;
    } catch (err) {
      sleep(100);
    }
  }
};

module.exports.get = async () => {
  const name = await Contract.methods.name().call();
  const symbol = await Contract.methods.symbol().call();
  const blockHeight = await web3.eth.getBlockNumber();
  var fromBlock = parseInt(Config.fromBlock) || 0;
  const blocksPerBatch = parseInt(Config.blocksPerBatch) || 0;
  const delay = parseInt(Config.delay) || 0;
  const toBlock = parseInt(Config.toBlock) || blockHeight;

  const lastDownloadedBlock = await LastDownloadedBlock.get(symbol);

  if (lastDownloadedBlock) {
    console.log("Resuming from the last downloaded block #", lastDownloadedBlock);
    fromBlock = lastDownloadedBlock + 1;
  }

  console.log("From %d to %d", fromBlock, toBlock);

  let start = fromBlock;
  let end = fromBlock;
  let i = 0;

  while (end < toBlock) {
    end = start + blocksPerBatch;
    
    if(end > toBlock) {
      end = toBlock;
    }
    
    i++;

    if (delay) {
      await sleep(delay);
    }

    console.log("Batch", i + 1, " From", start, "to", end);

    await tryGetEvents(start, end, symbol);

    start = end + 1;
    end = start;
  }

  // Hard disk writing is too slow, you need to wait, depending on the file system
  await sleep(1000);

  const events = await BlockReader.getEvents(symbol);

  const data = {
    name,
    symbol,
    events
  };

  return data;
};
