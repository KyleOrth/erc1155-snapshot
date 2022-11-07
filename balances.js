"use strict";

const Config = require("./config").getConfig();

module.exports.createBalances = async (data) => {
  const balances = new Map();
  const closingBalances = [];

  const onlyTrackMints = Config.onlyTrackMints;
  const tokenIdsToIgnoreSet = undefined;
  
  if(Config.tokenIdsToIgnore) {
    tokenIdsToIgnoreSet = new Set(Config.tokenIdsToIgnore);
  }

  const setDeposits = (event) => {
    const wallet = event.to;

    if(tokenIdsToIgnoreSet && tokenIdsToIgnoreSet.has(event.tokenId)) {
      return;
    }

    if(onlyTrackMints) {
      //Ignore deposit if it is not a mint
      //i.e. event.from must be equal to 0x000...
      //Otherwise its just a transfer
      if(event.from !== "0x0000000000000000000000000000000000000000") {
        return;
      }
    }

    let deposits = (balances.get(wallet) || {}).deposits || [];
    let withdrawals = (balances.get(wallet) || {}).withdrawals || [];

    if (!event.tokenId) {
      throw new TypeError("invalid tokenId value");
    }

    deposits = [...deposits, event.tokenId];
    balances.set(wallet, { deposits, withdrawals });
  };

  const setWithdrawals = (event) => {
    const wallet = event.from;

    if(tokenIdsToIgnoreSet && tokenIdsToIgnoreSet.has(event.tokenId)) {
      return;
    }

    if(onlyTrackMints) {
      //Ignore withdrawal if it is not a mint
      //i.e. event.from must be equal to 0x000...
      //Otherwise its just a transfer
      if(event.from !== "0x0000000000000000000000000000000000000000") {
        return;
      }
    }

    let deposits = (balances.get(wallet) || {}).deposits || [];
    let withdrawals = (balances.get(wallet) || {}).withdrawals || [];

    if (!event.tokenId) {
      throw new TypeError("invalid tokenId value");
    }

    withdrawals = [...withdrawals, event.tokenId];
    balances.set(wallet, { deposits, withdrawals });
  };

  for (const event of data.events) {
    setDeposits(event);
    setWithdrawals(event);
  }

  for (const [key, value] of balances.entries()) {
    if (key === "0x0000000000000000000000000000000000000000") {
      continue;
    }

    const tokenIds = [];
    const withdrawals = [].concat(value.withdrawals);
    for (let i = 0, l = value.deposits.length; i < l; i++) {
      const withdrawalIndex = withdrawals.indexOf(value.deposits[i]);
      if (withdrawalIndex === -1) {
        tokenIds.push(value.deposits[i]);
      } else {
        withdrawals.splice(withdrawalIndex, 1);
      }
    }

    closingBalances.push({
      wallet: key,
      tokenIds
    });
  }

  return closingBalances.filter((b) => b.tokenIds.length > 0);
};
