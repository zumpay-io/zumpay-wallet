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
const UUID = require('uuid/v4')

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

      /* Set up the RabbitMQ queues */
      await publicChannel.assertQueue(Config.queues.complete, {
        durable: true
      })
      await publicChannel.assertQueue(Config.queues.relayAgent, {
        durable: true
      })
      await privateChannel.assertQueue(Config.queues.send, {
        durable: true
      })

      privateChannel.prefetch(1)

      /* Create our worker's reply queue */
      const replyQueue = await publicChannel.assertQueue('', { exclusive: true, durable: false })

      privateChannel.consume(Config.queues.send, async function (message) {
        if (message !== null) {
          var payload = JSON.parse(message.content.toString())

          /* If the payload is encrypted, we need to decrypt it */
          if (payload.encrypted) {
            payload = crypto.decrypt(payload.encrypted)
          }

          var cancelTimer

          /* Sort our outputs from smallest to largest */
          payload.funds.sort((a, b) => (a.amount > b.amount) ? 1 : ((b.amount > a.amount) ? -1 : 0))

          /* Pull some of the information we need for further processing
             out of the outputs we received */
          var totalToSend = 0
          var amounts = []
          for (var i = 0; i < payload.funds.length; i++) {
            totalToSend += payload.funds[i].amount
            amounts.push(payload.funds[i].amount)
          }

          /* If there is less funds available than the network fee
             we can't really do anything. We're not going to pay to
             to send on funds we didn't receive enough for to at least
             cover our costs. */
          if (totalToSend < Config.defaultNetworkFee) {
            /* Generate the response for sending it back to the requestor,
                 to let them know that funds were received (still incomplete) */
            var goodResponse = {
              address: payload.wallet.address,
              status: 402, // Payment required (as in not enough to send anything on
              request: payload.request,
              privateKey: payload.privateKey
            }

            publicChannel.sendToQueue(Config.queues.complete, Buffer.from(JSON.stringify(goodResponse)), {
              persistent: true
            })

            /* We are going to close this request out because we're
               not going to do anything else with this */
            log(util.format('[INFO] Worker #%s encountered wallet with insufficient funds [%s]', cluster.worker.id, payload.wallet.address))
            return privateChannel.ack(message)
          }

          /* Save off the amount that we actually received */
          const amountReceived = totalToSend

          /* Deduct the network transaction fee */
          totalToSend -= Config.defaultNetworkFee

          /* If there's still funds to send, we need to create the necessary
             outputs to send them the funds. The way that createOutputs
             is implemented in the utils library, every transaction we create
             is ~almost~ a fusion transaction */
          var outputs = []
          if (totalToSend !== 0) {
            outputs = cryptoUtils.createTransactionOutputs(payload.request.address, totalToSend)
          }

          /* Let's go get our random outputs to mix with */
          var randomOutputs = []
          if (amounts.length !== 0) {
            randomOutputs = await request({
              url: Config.randomOutputsUrl,
              json: true,
              method: 'POST',
              body: {
                mixin: Config.defaultMixinCount,
                amounts: amounts
              }
            })
          }

          /* Validate that we received enough randomOutputs to complete the request */
          if (randomOutputs.length !== payload.funds.length) {
            /* Something didn't work right, let's leave this for someone else to handle */
            log(util.format('[INFO] Worker #%s encountered a request with an invalid number of random outputs [%s]', cluster.worker.id, payload.wallet.address))
            return privateChannel.nack(message)
          }

          /* Although the block API returns the random outputs sorted by amount,
             we're going to sort it ourselves quick just to make sure */
          randomOutputs.sort((a, b) => (a.amount > b.amount) ? 1 : ((b.amount > a.amount) ? -1 : 0))

          /* We received more random outputs than we actually need for each of the amounts
             just in case one of the outputs we received matches one we're trying
             to mix with. We need to make some sense of this before trying
             to build our transaction otherwise things are going to get very ugly. */

          /* We'll start looping through our outputs to run sanity checks */
          for (var j = 0; j < payload.funds.length; j++) {
            var saneRandoms = []
            var randoms = randomOutputs[j]

            /* If, for some reason the amounts of the random outputs
               aren't for this set of funds, something went wrong...
               very very wrong */
            if (randoms.amount !== payload.funds[j].amount) {
              throw new Error('Error handling random outputs')
            }

            /* Loop through the fake inputs to do some basic checking including
               that we actually need additional fake inputs */
            randoms.outs.forEach((elem) => {
              if (elem.out_key !== payload.funds[j].keyImage && saneRandoms.length < Config.defaultMixinCount) {
                /* Toss them in the stack in a way that the library we use
                   requires them */
                saneRandoms.push({
                  globalIndex: elem.global_amount_index,
                  key: elem.out_key
                })
              }
            })
            randomOutputs[j] = saneRandoms
          }

          /* Decode the payload address so that we can figure out if we need to send
             the funds with a payment ID */
          var decodedAddress = cryptoUtils.decodeAddress(payload.request.address)
          var paymentId = null
          if (decodedAddress.paymentId.length !== 0) {
            paymentId = decodedAddress.paymentId
          }

          /* Create the new transaction that will send the funds on */
          const tx = cryptoUtils.createTransaction(payload.wallet, outputs, payload.funds, randomOutputs, Config.defaultMixinCount, Config.defaultNetworkFee, paymentId)

          /* Generate a random request ID for use by the relay agent client */
          const requestId = UUID().toString().replace(/-/g, '')

          /* Here, we set up our worker side of the queue to grab the replyQueue
             from the relay agent workers so we can spit the results back to the client */
          publicChannel.consume(replyQueue.queue, async function (replyMessage) {
            /* If we received a valid message and it matches our request we're good to go */
            if (replyMessage !== null && replyMessage.properties.correlationId === requestId) {
              var workerResponse = JSON.parse(replyMessage.content.toString())

              /* Acknowledge that we received the response */
              publicChannel.ack(replyMessage)

              /* We received a response so we don't need this timer anymore */
              if (cancelTimer !== null) {
                clearTimeout(cancelTimer)
              }

              if (workerResponse.status && workerResponse.status.toUpperCase() === 'OK') {
                var response = {
                  address: payload.wallet.address,
                  keys: {
                    privateSpend: payload.wallet.spend.privateKey,
                    privateView: payload.wallet.view.privateKey
                  },
                  amountReceived: amountReceived,
                  amountSent: totalToSend,
                  networkFee: Config.defaultNetworkFee,
                  transactionHash: tx.hash,
                  transactionPrivateKey: tx.transaction.prvkey,
                  status: 200, // OK
                  request: payload.request,
                  privateKey: payload.privateKey
                }

                /* Send the 'completed' request information back
                   to the public processors so we can let the requestor
                   know that the process is complete */
                publicChannel.sendToQueue(Config.queues.complete, Buffer.from(JSON.stringify(response)), {
                  persistent: true
                })

                /* Success! Now get out of here as we're done */
                log(util.format('[INFO] Worker #%s relayed [%s] from [%s] to [%s] in transaction [%s]', cluster.worker.id, totalToSend, payload.wallet.address, payload.request.address, tx.hash))
                return privateChannel.ack(message)
              } else {
                /* For some reason, the relay failed, we'll send it back
                   to the queue to retry later */
                log(util.format('[INFO] Worker #%s failed to relay [%s] from [%s] to [%s]', cluster.worker.id, totalToSend, payload.wallet.address, payload.request.address))
                return privateChannel.nack(message)
              }
            } else if (replyMessage !== null) {
              /* There was a message, but it wasn't for us. Let it go back
                 in the queue for someone else to handle */
              return publicChannel.nack(replyMessage)
            }
          })

          /* Send the transaction to the relay workers and give it 5s to process */
          publicChannel.sendToQueue(Config.queues.relayAgent, Buffer.from(JSON.stringify(tx)), {
            correlationId: requestId,
            replyTo: replyQueue.queue,
            expiration: 5000
          })

          /* Set up a cancel timer in case things don't return properly from
             the relay agent */
          cancelTimer = setTimeout(() => {
            log(util.format('[INFO] Worker #%s request timed out when attempting to relay [%s] from [%s] to [%s]', cluster.worker.id, totalToSend, payload.wallet.address, payload.request.address))
            privateChannel.nack(message)
          }, 5500)
        }
      })
    } catch (e) {
      log(util.format('Error in worker #%s: %s', cluster.worker.id, e.toString()))
      cluster.worker.kill()
    }

    log(util.format('Worker #%s awaiting requests', cluster.worker.id))
  }())
}
