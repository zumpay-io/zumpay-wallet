// Copyright (c) 2019 ZumPay Development Team
//
// Please see the included LICENSE file for more information.

'use strict'

const childProcess = require('child_process')
const startCreator = (process.argv.length >= 3 && process.argv[2].toLowerCase() === 'creator')
const startScanner = (process.argv.length >= 3 && process.argv[2].toLowerCase() === 'scanner')
const startSender = (process.argv.length >= 3 && process.argv[2].toLowerCase() === 'sender')
const startAll = (!startCreator && !startScanner && !startSender)
var currentColor = 31

/* Copy these into environment variables so that we
   can pass them to our child workers */
const env = {
  RABBIT_PUBLIC_SERVER: process.env.RABBIT_PUBLIC_SERVER || 'localhost',
  RABBIT_PUBLIC_USERNAME: process.env.RABBIT_PUBLIC_USERNAME || '',
  RABBIT_PUBLIC_PASSWORD: process.env.RABBIT_PUBLIC_PASSWORD || '',
  RABBIT_PRIVATE_SERVER: process.env.RABBIT_PRIVATE_SERVER || 'localhost',
  RABBIT_PRIVATE_USERNAME: process.env.RABBIT_PRIVATE_USERNAME || '',
  RABBIT_PRIVATE_PASSWORD: process.env.RABBIT_PRIVATE_PASSWORD || '',
  RABBIT_PRIVATE_ENCRYPTION_KEY: process.env.RABBIT_PRIVATE_ENCRYPTION_KEY || ''
}

const spawn = function (name, script, color) {
  if (!color) {
    color = currentColor
    currentColor++
  }
  const labelColor = `\x1b[${color}m`
  var child = childProcess.spawn('node', [ script ], {
    cwd: process.cwd(),
    env: env
  })
  child.stdout.on('data', (data) => {
    data.toString().trim().split(/\r?\n/).forEach((message) => {
      console.log('%s[%s] %s', labelColor, name, message.trim())
    })
  })
  child.on('exit', () => {
    spawn(name, script, color)
  })
}

console.log('Starting ZumPay wallet workers...')

/* Spawn the wallet creator */
if (startCreator || startAll) { spawn('CREATOR', 'walletCreator.js') }

/* Spawn the wallet scanner */
if (startScanner || startAll) { spawn('SCANNER', 'walletScanner.js') }

/* Spawn the wallet sender */
if (startSender || startAll) { spawn('SENDER ', 'walletSender.js') }
