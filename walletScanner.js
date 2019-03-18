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

const cryptoUtils = new ZumCoinUtils()
const cpuCount = require('os').cpus().length

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
      var publicRabbit = await RabbitMQ.connect(buildConnectionString(publicRabbitHost, publicRabbitUsername, publicRabbitPassword))
      var publicChannel = await publicRabbit.createChannel()

      var privateRabbit = await RabbitMQ.connect(buildConnectionString(privateRabbitHost, privateRabbitUsername, privateRabbitPassword))
      var privateChannel = await privateRabbit.createChannel()

      await publicChannel.assertQueue(Config.queues.complete, {
        durable: true
      })
      await privateChannel.assertQueue(Config.queues.send, {
        durable: true
      })

      privateChannel.prefetch(1)

      privateChannel.consume(Config.queues.scan, async function (message) {
        if (message !== null) {
          var payload = JSON.parse(message.content.toString())

          /* If the payload is encrypted, we need to decrypt it */
          if (payload.encrypted) {
            payload = crypto.decrypt(payload.encrypted)
          }

          var confirmationsRequired = (typeof payload.request.confirmations !== 'undefined') ? payload.request.confirmations : Config.defaultConfirmations

          /* If someone somehow manages to send us through a request
             with a very long number of confirmations we could end
             up with a job stuck in our queue forever */
          if (confirmationsRequired > Config.maximumConfirmations) {
            confirmationsRequired = Config.maximumConfirmations
          }

          /* Let's get some basic block information regarding our wallet */
          var topBlock
          try {
            topBlock = await request({ url: Config.blockHeaderUrl + 'top', json: true })
          } catch (e) {
            /* If we can't grab this information, then something went wrong and we need
               to leave this for someone else to handle */
            log(util.format('[ERROR] Worker #%s encountered an error retrieving block data', cluster.worker.id))
            return privateChannel.nack(message)
          }

          /* If we're at the same block height as when the request was
             created, then there's 0% chance that our transaction has
             occurred */
          if (topBlock.height === payload.scanHeight) {
            /* We need to wait a little bit before we signal back that this request
               needs handled again to avoid log spam and conserve bandwidth */
            return setTimeout(() => {
              /* Let Rabbit know that this request needs to be handled again */
              return privateChannel.nack(message)
            }, 5000)
          }

          /* Let's go get blockchain transactional data so we can scan through it */
          var syncData
          try {
            syncData = await request({ url: Config.syncUrl, json: true, method: 'POST', body: { scanHeight: payload.scanHeight, blockCount: (Config.maximumScanBlocks + 1) } })
          } catch (e) {
            /* That didn't work out well, let's just leave this for someone else */
            log(util.format('[ERROR] Worker #%s could not retrieve sync data for wallet [%s]', cluster.worker.id, payload.wallet.address))
            return privateChannel.nack(message)
          }

          /* We'll store our outputs in here */
          var walletOutputs = []
          var totalAmount = 0
          var fundsFoundInBlock = 0

          /* Loop through the returned blocks */
          for (var i = 0; i < syncData.length; i++) {
            var block = syncData[i]

            /* Loop through transactions in the block */
            for (var j = 0; j < block.transactions.length; j++) {
              var transaction = block.transactions[j]

              /* Check to see if any of the outputs in the transaction belong to us */
              var outputs = cryptoUtils.scanTransactionOutputs(transaction.publicKey, transaction.outputs, payload.wallet.view.privateKey, payload.wallet.spend.publicKey, payload.wallet.spend.privateKey)

              /* If we found outputs, we need to store the top block height we found
                 the funds in so we know where to start our confirmation check from */
              if (outputs.length !== 0 && block.height > fundsFoundInBlock) {
                fundsFoundInBlock = block.height
              }

              /* Loop through any found outputs and start tallying them up */
              for (var l = 0; l < outputs.length; l++) {
                totalAmount += outputs[l].amount
                walletOutputs.push(outputs[l])
              }
            }
          }

          /* Did we find some outputs for us? */
          if (walletOutputs.length > 0) {
            /* Did we find all the funds we requested and do we have the required confirmations? */
            if (totalAmount >= payload.request.amount && (topBlock.height - fundsFoundInBlock) >= confirmationsRequired) {
              /* Congrats, we found all the funds that we requested and we're ready
                 to send them on */

              /* Generate the response for sending it back to the requestor,
                 to let them know that funds were received (still incomplete) */
              var goodResponse = {
                address: payload.wallet.address,
                amount: totalAmount,
                status: 100, // Continue
                request: payload.request,
                privateKey: payload.privateKey
              }

              publicChannel.sendToQueue(Config.queues.complete, Buffer.from(JSON.stringify(goodResponse)), {
                persistent: true
              })

              /* Stick our funds in our payload */
              payload.funds = walletOutputs

              /* First we encrypt the object that we are going to send to our queues */
              const encryptedPayload = { encrypted: crypto.encrypt(payload) }

              /* Signal to the workers who send the funds to their real destination that things are ready */
              privateChannel.sendToQueue(Config.queues.send, Buffer.from(JSON.stringify(encryptedPayload)), {
                persistent: true
              })

              /* This request is now complete from the scanning standpoint */
              log(util.format('[INFO] Worker #%s found %s for [%s] and is forwarding request to send workers', cluster.worker.id, totalAmount, payload.wallet.address))
              return privateChannel.ack(message)
            } else if (totalAmount >= payload.request.amount) {
              /* We found all the funds we need, but we don't have enough confirmations yet */
              log(util.format('[INFO] Worker #%s found %s for [%s] but is awaiting confirmations. %s blocks to go', cluster.worker.id, totalAmount, payload.wallet.address, (confirmationsRequired - (topBlock.height - fundsFoundInBlock))))

              var confirmationsRemaining = confirmationsRequired - (topBlock.height - fundsFoundInBlock)
              if (confirmationsRemaining < 0) {
                confirmationsRemaining = 0
              }

              /* We need to let everyone know that we found their monies but we need more confirmations */
              var waitingForConfirmations = {
                address: payload.wallet.address,
                amount: totalAmount,
                confirmationsRemaining: confirmationsRemaining,
                status: 102,
                request: payload.request,
                privateKey: payload.privateKey
              }

              publicChannel.sendToQueue(Config.queues.complete, Buffer.from(JSON.stringify(waitingForConfirmations)), {
                persistent: true
              })

              /* We need to wait a little bit before we signal back that this request
                 needs handled again to avoid log spam and conserve bandwidth */
              return setTimeout(() => {
                /* Let Rabbit know that this request needs to be handled again */
                return privateChannel.nack(message)
              }, 5000)
            } else if (topBlock.height > payload.maxHeight && (topBlock.height - fundsFoundInBlock) >= confirmationsRequired) {
              /* We found founds but it's not at least the amount that we requested
                 unfortunately, we've also ran out of time to look for more */

              /* Build a response that we can send back to the requestor to let them know
                 that we've received some funds, but not all of them, but that their
                 request has been timed out */
              var partialResponse = {
                address: payload.wallet.address,
                status: 206, // Partial Content (aka Partial Payment)
                request: payload.request,
                privateKey: payload.privateKey
              }

              publicChannel.sendToQueue(Config.queues.complete, Buffer.from(JSON.stringify(partialResponse)), {
                persistent: true
              })

              /* Stick our funds in our payload */
              payload.funds = walletOutputs

              /* First we encrypt the object that we are going to send to our queues */
              const encryptedPayload = { encrypted: crypto.encrypt(payload) }

              /* Signal to the workers who send the funds to their real destination that things are ready */
              privateChannel.sendToQueue(Config.queues.send, Buffer.from(JSON.stringify(encryptedPayload)), {
                persistent: true
              })

              /* This request is now complete from the scanning standpoint */
              log(util.format('[INFO] Worker #%s found %s for [%s] and is forwarding request to send workers', cluster.worker.id, totalAmount, payload.wallet.address))
              return privateChannel.ack(message)
            } else {
              /* We found some funds, it's not what we're looking for but we still have time
                 to keep looking for more */
              log(util.format('[INFO] Worker #%s found %s for [%s] but we need to look for more', cluster.worker.id, totalAmount, payload.wallet.address))

              var blocksRemaining = (payload.maxHeight - topBlock.height)
              if (blocksRemaining < 0) {
                blocksRemaining = 0
              }

              /* We need to let everyone know that we found some monies but we need more */
              var waitingForConfirmationsNotEnough = {
                address: payload.wallet.address,
                amount: totalAmount,
                blocksRemaining: blocksRemaining,
                status: 102,
                request: payload.request,
                privateKey: payload.privateKey
              }

              publicChannel.sendToQueue(Config.queues.complete, Buffer.from(JSON.stringify(waitingForConfirmationsNotEnough)), {
                persistent: true
              })

              /* We need to wait a little bit before we signal back that this request
                 needs handled again to avoid log spam and conserve bandwidth */
              return setTimeout(() => {
                /* Let Rabbit know that this request needs to be handled again */
                return privateChannel.nack(message)
              }, 5000)
            }
          }

          /* We need to observe the maximum amount of time
             that we are going to look for transactions if we
             don't find anything that we're looking for */
          if (topBlock.height > payload.maxHeight && walletOutputs.length === 0) {
            var response = {
              address: payload.wallet.address,
              keys: {
                privateSpend: payload.wallet.spend.privateKey,
                privateView: payload.wallet.view.privateKey
              },
              status: 408, // Request Timed Out
              request: payload.request,
              privateKey: payload.privateKey
            }

            /* Send the 'cancelled' wallet back to the public
               workers that will signal to the caller that the
               request has been abandoned */
            publicChannel.sendToQueue(Config.queues.complete, Buffer.from(JSON.stringify(response)), {
              persistent: true
            })

            /* That's it, we're done with this request */
            log(util.format('[INFO] Worker #%s timed out wallet [%s]', cluster.worker.id, payload.wallet.address))
            return privateChannel.ack(message)
          }

          /* If our request has not been timed out (cancelled) and
             we didn't find our funds yet, then let's throw it
             back in the queue for checking again later */

          /* We need to wait a little bit before we signal back that this request
             needs handled again to avoid log spam and conserve bandwidth */
          return setTimeout(() => {
            /* Let Rabbit know that this request needs to be handled again */
            return privateChannel.nack(message)
          }, 5000)
        }
      })
    } catch (e) {
      log(util.format('Error in worker #%s: %s', cluster.worker.id, e.toString()))
      cluster.worker.kill()
    }

    log(util.format('Worker #%s awaiting requests', cluster.worker.id))
  }())
}
