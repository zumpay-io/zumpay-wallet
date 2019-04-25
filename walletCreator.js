// Copyright (c) 2018, TurtlePay Developers
// Copyright (c) 2019 ZumPay Development Team
//
// Please see the included LICENSE file for more information.

'use strict'

const Config = require('./config.json')
const ZumCoinUtils = require('zumcoin-utils')
const request = require('request-promise-native')
const RabbitMQ = require('amqplib')
const cluster = require('cluster')
const util = require('util')
const AES = require('./lib/aes.js')
const MessageSigner = require('./lib/messageSigner.js')
const signer = new MessageSigner()

const cryptoUtils = new ZumCoinUtils()
const cpuCount = require('os').cpus().length
const topBlockUrl = Config.blockHeaderUrl + 'top'

const publicRabbitHost = process.env.RABBIT_PUBLIC_SERVER || 'localhost'
const publicRabbitUsername = process.env.RABBIT_PUBLIC_USERNAME || ''
const publicRabbitPassword = process.env.RABBIT_PUBLIC_PASSWORD || ''

const privateRabbitHost = process.env.RABBIT_PRIVATE_SERVER || 'localhost'
const privateRabbitUsername = process.env.RABBIT_PRIVATE_USERNAME || ''
const privateRabbitPassword = process.env.RABBIT_PRIVATE_PASSWORD || ''
const privateRabbitEncryptionKey = process.env.RABBIT_PRIVATE_ENCRYPTION_KEY || ''

function log (message) {
  console.log(util.format('%s: %s', (new Date()).toUTCString(), message))
}

function spawnNewWorker () {
  cluster.fork()
}

/* Helps us to build the RabbitMQ connection string */
function buildConnectionString (host, username, password) {
  log(util.format('Setting up connection to %s@%s...', username, host))
  var result = ['amqp://']

  if (username.length !== 0 && password.length !== 0) {
    result.push(username + ':')
    result.push(password + '@')
  }

  result.push(host)

  return result.join('')
}

if (cluster.isMaster) {
  for (var cpuThread = 0; cpuThread < cpuCount; cpuThread++) {
    spawnNewWorker()
  }

  cluster.on('exit', (worker, code, signal) => {
    log(util.format('worker %s died', worker.process.pid))
    spawnNewWorker()
  })
} else if (cluster.isWorker) {
  (async function () {
    try {
      const crypto = new AES({ password: privateRabbitEncryptionKey })

      /* Set up our access to the necessary RabbitMQ systems */
      var incoming = await RabbitMQ.connect(buildConnectionString(publicRabbitHost, publicRabbitUsername, publicRabbitPassword))
      var incomingChannel = await incoming.createChannel()

      var outgoing = await RabbitMQ.connect(buildConnectionString(privateRabbitHost, privateRabbitUsername, privateRabbitPassword))
      var outgoingChannel = await outgoing.createChannel()

      await incomingChannel.assertQueue(Config.queues.new)
      await outgoingChannel.assertQueue(Config.queues.scan, {
        durable: true
      })

      incomingChannel.prefetch(1)

      /* Let's get any new wallet requests from the public facing API */
      incomingChannel.consume(Config.queues.new, (message) => {
        /* Is the message real? */
        if (message !== null) {
          /* Generate a new address, nothing fancy going on here, just a brand new address */
          var newAddress = cryptoUtils.createNewAddress()

          /* We need to go get some information about the current top block */
          request({
            url: topBlockUrl,
            json: true
          }).then(async function (topBlock) {
            /* Now that we know what our scan height is, we can get moving */
            log(util.format('Worker #%s: Created new wallet %s at height %s for %s', cluster.worker.id, newAddress.address, topBlock.height, message.properties.correlationId))

            /* Get a random set of keys for message signing */
            const keys = await signer.generateKeys()

            /* Construct our response back to the public API -- non-sensitive information */
            const response = {
              address: newAddress.address,
              scanHeight: topBlock.height,
              maxHeight: (topBlock.height + Config.maximumScanBlocks),
              publicKey: keys.publicKey
            }

            /* Go ahead and fire that information back to the public API for the related request */
            incomingChannel.sendToQueue(message.properties.replyTo, Buffer.from(JSON.stringify(response)), {
              correlationId: message.properties.correlationId
            })

            /* Construct our payload for sending via the private RabbitMQ server
               to the workers that are actually scanning the wallets for incoming
               funds */
            const scanPayload = {
              wallet: newAddress,
              scanHeight: topBlock.height,
              maxHeight: (topBlock.height + Config.maximumScanBlocks),
              request: JSON.parse(message.content.toString()),
              privateKey: keys.privateKey
            }

            /* First, we encrypt the object that we are going to send to our queues */
            const encryptedPayload = { encrypted: crypto.encrypt(scanPayload) }

            /* Go ahead and send that message to the scanning queue */
            outgoingChannel.sendToQueue(Config.queues.scan, Buffer.from(JSON.stringify(encryptedPayload)), { persistent: true })

            /* Acknowledge that we've handled this new wallet request */
            incomingChannel.ack(message)
          }).catch((error) => {
            /* If we weren't able to retrieve what the current top block is (height) then we're bailing */
            log(util.format('Worker #%s: Could not create new wallet [%s]', cluster.worker.id, error.toString()))

            /* Someone else can handle this request */
            incomingChannel.nack(message)
          })
        }
      })
    } catch (e) {
      /* Something went horribly wrong and this worker needs to go */
      log(util.format('Error in worker #%s: %s', cluster.worker.id, e.toString()))
      cluster.worker.kill()
    }

    log(util.format('Worker #%s awaiting requests', cluster.worker.id))
  }())
}
