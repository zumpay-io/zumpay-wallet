// Copyright (c) 2018, TurtlePay Developers
// Copyright (c) 2019 ZumPay Development Team
//
// Please see the included LICENSE file for more information.

'use strict'

const crypto = require('crypto')
const pem = require('pem')

function Self (opts) {
  opts = opts || {}
  if (!(this instanceof Self)) return new Self(opts)
  this.algo = opts.algo || 'sha256'
  this.keySize = opts.size || 2048
}

Self.prototype.sign = function (message, privateKey) {
  return new Promise((resolve, reject) => {
    try {
      message = JSON.stringify(message)
      privateKey = Buffer.from(privateKey, 'hex').toString()
      const signer = crypto.createSign(this.algo)
      signer.update(message)
      signer.end()

      const signature = signer.sign(privateKey).toString('hex')

      return resolve(signature)
    } catch (error) {
      return reject(error)
    }
  })
}

Self.prototype.verify = function (message, publicKey, signature) {
  return new Promise((resolve, reject) => {
    try {
      message = JSON.stringify(message)
      publicKey = Buffer.from(publicKey, 'hex').toString()
      signature = Buffer.from(signature, 'hex')

      const verifier = crypto.createVerify(this.algo)
      verifier.update(message)
      verifier.end()

      const verified = verifier.verify(publicKey, signature)

      return resolve(verified)
    } catch (error) {
      return reject(error)
    }
  })
}

Self.prototype.generateKeys = function () {
  return new Promise((resolve, reject) => {
    pem.createPrivateKey(2048, {}, (error, privateKey) => {
      if (error) return reject(error)
      pem.getPublicKey(privateKey.key, (error, publicKey) => {
        if (error) return reject(error)
        try {
          return resolve({ privateKey: Buffer.from(privateKey.key).toString('hex'), publicKey: Buffer.from(publicKey.publicKey).toString('hex') })
        } catch (error) {
          return reject(error)
        }
      })
    })
  })
}

module.exports = Self
