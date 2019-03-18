// Copyright (c) 2019 ZumPay Development Team
//
// Please see the included LICENSE file for more information.

'use strict'

const crypto = require('crypto')
const AES = require('aes-js')

function Self (opts) {
  opts = opts || {}
  if (!(this instanceof Self)) return new Self(opts)
  if (!opts.password) throw new Error('must supply a password')
  this.key = Buffer.from(crypto.createHmac('sha256', opts.password).digest('hex'), 'hex')
}

Self.prototype.encrypt = function (data, json) {
  json = json || true
  if (json) {
    data = JSON.stringify(data)
  }
  const dataBytes = AES.utils.utf8.toBytes(data)
  const aesCtr = new AES.ModeOfOperation.ctr(this.key)
  const encryptedBytes = aesCtr.encrypt(dataBytes)
  return AES.utils.hex.fromBytes(encryptedBytes)
}

Self.prototype.decrypt = function (data, json) {
  json = json || true
  const encryptedBytes = AES.utils.hex.toBytes(data)
  const aesCtr = new AES.ModeOfOperation.ctr(this.key)
  const decryptedBytes = aesCtr.decrypt(encryptedBytes)
  const decryptedText = AES.utils.utf8.fromBytes(decryptedBytes)
  if (json) {
    return JSON.parse(decryptedText)
  } else {
    return decryptedText
  }
}

module.exports = Self
