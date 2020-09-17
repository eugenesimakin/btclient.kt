package edu.bittorrent.client

import com.github.pbbl.heap.ByteBufferPool
import org.apache.commons.io.IOUtils
import java.io.BufferedReader
import java.io.ByteArrayInputStream
import java.io.FileInputStream
import java.io.InputStreamReader
import java.net.InetSocketAddress
import java.net.Socket
import java.net.URI
import java.net.URL
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.FileSystems
import java.nio.file.StandardOpenOption.*
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.Executors
import java.util.concurrent.ThreadFactory
import java.util.concurrent.atomic.AtomicInteger
import kotlin.concurrent.thread
import kotlin.system.exitProcess

const val CLIENT_ID = "-BB4821-706189213633" // this client peer_id
const val MESSAGES_EXCHANGING_THREADS = 10
const val BLOCK_PIECE_SIZE = 16384 // bytes

const val U_INT_SIZE = 4 // bytes
const val MSG_ID_SIZE = 1 // bytes

interface PeerSocketChannel {
    fun sendPendingMessages()
    fun receiveMessages()
    fun close()
}

fun main(args: Array<String>) {
    if (args.isEmpty()) {
        error("No torrent file provided")
    }

    val filename = args[0]
    val torrentFilePath = FileSystems.getDefault().getPath(filename)
    if (!torrentFilePath.toFile().exists()) {
        error("Torrent file doesn't exist")
    }

    val metaInfoBytes = IOUtils.toByteArray(FileInputStream(torrentFilePath.toFile()))
    val metaInfo = MetaInfo(metaInfoBytes)
    val httpTrackersUrls = metaInfo.announceUrls.filter { it.startsWith("http://") }

    if (httpTrackersUrls.isEmpty()) {
        error("No http trackers available in the torrent")
    }

    var peerAddresses: MutableSet<InetSocketAddress> = httpTrackersUrls
            .map { formatUrlWith(metaInfo, it) }
            .map { URI.create(it).toURL() }
            .map {
                if (it.port > 0) {
                    it
                } else {
                    URL(it.protocol, it.host, 80, it.file)
                }
            }
            .flatMap { url ->
                try {
                    val trackerResponse = requestPeersAsync(url)
                    if (trackerResponse.failureReason() == null) {
                        trackerResponse.peersAddresses()
                    } else {
                        println("Error (failure) requesting peers from $url: ${trackerResponse.failureReason()}")
                        emptyList()
                    }
                } catch (ex: Exception) {
                    println("Error requesting peers from $url: ${ex.message}")
                    emptyList<InetSocketAddress>()
                }
            }.toMutableSet()

    if (peerAddresses.isEmpty()) error("No peers has been received")

    val messagesExchangingQueue = ArrayBlockingQueue<PeerSocketChannel>(peerAddresses.size + 1)
    val peersQueue = ArrayBlockingQueue<Peer>(peerAddresses.size)
    val absentPiecesQueue = ArrayBlockingQueue<PieceInfo>(metaInfo.pieces.size * 2)
    val donePieceOfWorksQueue = ArrayBlockingQueue<DonePieceOfWork>(metaInfo.pieces.size * 2)

    val msgExchangingExec = Executors.newFixedThreadPool(MESSAGES_EXCHANGING_THREADS, MessagesThreadFactory())
    for (i in 0..MESSAGES_EXCHANGING_THREADS) {
        msgExchangingExec.execute {
            while (!Thread.currentThread().isInterrupted) {
                val peer = messagesExchangingQueue.poll() ?: continue
                try {
                    peer.sendPendingMessages()
                    peer.receiveMessages()
                    messagesExchangingQueue.offer(peer)
                } catch (ex: IllegalStateException) {
                    peersQueue.remove(peer)
                    peer.close()
                } catch (ex: Exception) {
                    ex.printStackTrace()
                    peersQueue.remove(peer)
                    peer.close()
                }
            }
        }
    }

    val bufferPool = ByteBufferPool()
    val absentPoisonPiece = PieceInfo(-1, ByteArray(0))
    val donePoisonPieceOfW = DonePieceOfWork(PieceInfo(-1, ByteArray(0)), ByteArray(0))

    val failedConnectionsCount = AtomicInteger(0)
    val connExec = Executors.newCachedThreadPool()
    for (address in peerAddresses) {
        connExec.execute {
            try {
                val peer = Peer.connectAndHandshake(address, metaInfo, absentPiecesQueue, donePieceOfWorksQueue, bufferPool)
                messagesExchangingQueue.add(peer)
                peersQueue.add(peer)
            } catch (ex: Exception) {
//                ex.printStackTrace()
                val failed = failedConnectionsCount.incrementAndGet()
                println("Failed ($failed) to connect to Peer[$address] - ${ex.message}")
                if (failed == peerAddresses.size) {
                    donePieceOfWorksQueue.offer(donePoisonPieceOfW)
                }
            }
        }
    }

    metaInfo.pieces.shuffled().forEach { absentPiecesQueue.add(it) }

    thread(start = true, name = "AbsentPiecesPolling", isDaemon = true) {
        while (!Thread.currentThread().isInterrupted) {
            val piece = absentPiecesQueue.poll() ?: continue
            if (piece == absentPoisonPiece) break

            var match = false
            var i = 0
            while (!match && i < peersQueue.size) {
                val peer = peersQueue.poll() ?: break
                match = peer.requestPiece(piece)
                peersQueue.offer(peer)
                if (match) {
                    break
                }
                i++
            }

            if (!match) {
                absentPiecesQueue.offer(piece)
            }
        }
    }

    val path = FileSystems.getDefault().getPath(metaInfo.name)
    if (path.toFile().exists()) path.toFile().delete()
    var pieceCounter = 0
    FileChannel.open(path, CREATE_NEW, READ, WRITE).use { fc ->
        try {
            while (true) {
                val done = donePieceOfWorksQueue.poll() ?: continue
                if (done == donePoisonPieceOfW) break

                val buf = ByteBuffer.wrap(done.data)

                val pos = calcPiecePosition(metaInfo, done.piece.index)
                fc.position(pos)
                while (buf.hasRemaining()) fc.write(buf)

                pieceCounter++
                val donePercent = ((pieceCounter.toDouble() * 100) / metaInfo.pieces.size.toDouble())
                println(String.format("%.2f percent done...", donePercent))
                if (pieceCounter >= metaInfo.pieces.size) {
                    break
                }
            }
        } finally {
            connExec.shutdown()
            msgExchangingExec.shutdown()
            bufferPool.close()
            absentPiecesQueue.offer(absentPoisonPiece)
            exitProcess(0)
        }
    }
}

private fun formatUrlWith(metaInfo: MetaInfo, url: String): String {
    val incomingPort = 9000
    return "$url?info_hash=${metaInfo.infoHashEncoded}&peer_id=$CLIENT_ID&port=$incomingPort&event=started&" +
            "uploaded=0&downloaded=0&compact=1&left=${metaInfo.length}&no_peer_id=0"
}

private fun requestPeersAsync(url: URL): TrackerResponse {
    val socket = Socket(url.host, url.port)

    val output = socket.getOutputStream()
    output.write("GET ${url.path}?${url.query} HTTP/1.1\r\n".toByteArray())
    output.write("Host: ${url.host}\r\n\r\n".toByteArray())

    val input = socket.getInputStream()

    var buf = ByteBuffer.allocate(10240000) // hope it'll be enough to not care about it
    while (true) {
        val r = input.read()
        if (r == -1) break
        buf.put(r.toByte())
    }

    val buffered = BufferedReader(InputStreamReader(ByteArrayInputStream(buf.array())))

    val firstLine = buffered.readLine()
    if (firstLine != "HTTP/1.1 200 OK") {
        error("Response from ${url.host} is $firstLine")
    }

    /*skip the http headers*/
    while (buffered.readLine() != "") {
    }

    buf = ByteBuffer.allocate(10240000) // hope it'll be enough to not care about it
    while (true) {
        val r = buffered.read()
        if (r == -1) break
        buf.put(r.toByte())
    }

    return TrackerResponse.parse(buf.array())
}

private fun calcPiecePosition(metaInfo: MetaInfo, index: Int): Long = index * metaInfo.pieceLength

class MessagesThreadFactory : ThreadFactory {
    override fun newThread(r: Runnable): Thread {
        val thread = Executors.defaultThreadFactory().newThread(r)
        thread.setUncaughtExceptionHandler { t, e ->
            System.err.println("Error: ${e.message}")
        }
        return thread
    }
}

data class DonePieceOfWork(val piece: PieceInfo, val data: ByteArray) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as DonePieceOfWork

        if (piece != other.piece) return false
        if (!data.contentEquals(other.data)) return false

        return true
    }

    override fun hashCode(): Int {
        var result = piece.hashCode()
        result = 31 * result + data.contentHashCode()
        return result
    }
}
