package edu.bittorrent.client

import com.github.pbbl.heap.ByteBufferPool
import org.apache.commons.codec.digest.DigestUtils
import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.channels.SocketChannel
import java.util.*
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.BlockingQueue
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

const val MB = 1_048_576
const val KB = 1_024

class Peer(
        private val channel: SocketChannel,
        private val absentPieces: BlockingQueue<PieceInfo>,
        private val donePieceOfWorks: BlockingQueue<DonePieceOfWork>,
        private val metaInfo: MetaInfo,
        private val bufferPool: ByteBufferPool
) : PeerSocketChannel {

    private val pendingMessagesQueue = ArrayBlockingQueue<Message>(3)

    // the byte buffers should have backed arrays (to use it below)
    private var writeBuf = ByteBuffer.wrap(ByteArray(2 * KB))
    private var readBuf = ByteBuffer.wrap(ByteArray(32 * KB))
    private var bytesRead = 0

    init {
        writeBuf.flip()
        readBuf.clear()
    }

    private val lock = ReentrantLock()
    private val bitField: BitSet = BitSet(metaInfo.pieces.size)

    private val peerChoking = AtomicBoolean(true)

    private val requestingPiece = AtomicBoolean(false)
    private var pieceBuf: ByteBuffer? = null
    private var currentPiece: PieceInfo? = null

    fun requestPiece(piece: PieceInfo): Boolean {
        if (!channel.isConnected) return false
        if (peerChoking.get()) return false
        if (requestingPiece.get()) return false

        lock.withLock {
            if (!bitField.get(piece.index)) {
                return false
            }
        }

        // we have the piece
        requestingPiece.set(true)

        pieceBuf = bufferPool.take(metaInfo.pieceLength.toInt())
        currentPiece = piece

        val blockPiece = BlockPieceInfo(piece.index, 0, BLOCK_PIECE_SIZE)
        pendingMessagesQueue.offer(Request(blockPiece))

        return true
    }

    private fun pieceSize() = metaInfo.length.toInt() / metaInfo.pieces.size

    override fun sendPendingMessages() {
        if (!channel.isOpen || !channel.isConnected) {
            error("Peer is not connected")
        }

        if (peerChoking.get() && requestingPiece.get()) {
            error("Peer is choking you while requesting a piece")
        }

        if (writeBuf.hasRemaining()) {
            channel.write(writeBuf) // writeBuf.get
            return
        }

        val messages = mutableListOf<Message>()
        pendingMessagesQueue.drainTo(messages)
        if (messages.isEmpty()) return

        // writeBuf has been read fully and ready to be filled
        writeBuf.clear()

        messages.forEach {
            val bytes = it.toByteArrayWithoutLength()
            val msgRaw = sizeOfByteArray(bytes) + bytes
            if (writeBuf.remaining() >= msgRaw.size) {
                writeBuf.put(msgRaw)
            } else {
                pendingMessagesQueue.offer(it)
            }
        }

        // writeBuf is ready to be read at the next invocation
        writeBuf.flip()
    }

    override fun receiveMessages() {
        if (!channel.isOpen || !channel.isConnected) {
            error("Peer is not connected")
        }

        val r = channel.read(readBuf) // readBuf.put
        if (r == -1) {
            error("Unexpected EOF")
        }

        bytesRead += r

        while (bytesRead >= U_INT_SIZE) {
            // we have msgLen if true
            val msgLenArr: ByteArray = readBuf.array().sliceArray(0 until U_INT_SIZE)
            assert(msgLenArr.size == 4)
            val msgLen = ByteBuffer.wrap(msgLenArr).int

            if (readBuf.position() - U_INT_SIZE >= msgLen && msgLen > 0) {
                // we have a message in buffer
                val msgBuf = bufferPool.take(msgLen + U_INT_SIZE)
                bytesRead -= (msgLen + U_INT_SIZE)

                readBuf.flip()
                while (msgBuf.hasRemaining()) msgBuf.put(readBuf.get())

                val msg = Message.fromBytes(msgBuf)
                onMessage(msg)
                bufferPool.give(msgBuf)

                val theRest = bufferPool.take(readBuf.remaining())
                while (readBuf.hasRemaining()) theRest.put(readBuf.get())

                readBuf.clear()
                theRest.flip()
                while (theRest.hasRemaining()) readBuf.put(theRest.get())
                bufferPool.give(theRest)
            } else {
                break
            }
        }
    }

    private fun onMessage(msg: Message) {
        when (msg.id()) {
            MsgId.Bitfield -> {
                val bitfieldMsg = msg as BitField
                val newBF = bitfieldMsg.bitField()
                lock.withLock {
                    for (i in 0..newBF.size()) {
                        bitField.set(i, newBF.get(i))
                    }
                }
                pendingMessagesQueue.offer(UnChoke())
                pendingMessagesQueue.offer(Interested())
            }
            MsgId.Have -> {
                val haveMsg = msg as Have
                val i = haveMsg.pieceIndex()
                lock.withLock {
                    bitField.set(i)
                }
            }
            MsgId.Choke -> {
                peerChoking.set(true)
            }
            MsgId.UnChoke -> {
                peerChoking.set(false)
            }
            MsgId.Piece -> {
                val pieceMsg = msg as Piece
                pieceBuf!!.put(pieceMsg.data())
                val nextBlockPiece = nextBlockInPiece(pieceMsg.info())
                if (nextBlockPiece != null) {
                    pendingMessagesQueue.offer(Request(nextBlockPiece))
                } else {
                    val data = pieceBuf!!.array()
                    val actualHash = DigestUtils.sha1(data)
                    if (currentPiece!!.hash.contentEquals(actualHash)) {
                        val pieceOfW = DonePieceOfWork(currentPiece!!, data)
                        donePieceOfWorks.offer(pieceOfW)
                    } else {
                        println("Failed to verify piece #${currentPiece!!.index} by hash")
                        absentPieces.offer(currentPiece!!)
                    }
                    bufferPool.give(pieceBuf)
                    pieceBuf = null
                    currentPiece = null
                    requestingPiece.set(false)
                }
            }
            else -> {
                // ignored
            }
        }
    }

    private fun nextBlockInPiece(old: BlockPieceInfo): BlockPieceInfo? {
        val pieceSize = pieceSize()
        val nextBegin = old.begin + old.length
        return if (nextBegin > pieceSize) {
            null
        } else {
            val left = if ((nextBegin + old.length) <= pieceSize) {
                old.length
            } else {
                pieceSize - nextBegin
            }
            if (left <= 0) {
                null
            } else {
                BlockPieceInfo(old.index, nextBegin, left)
            }
        }
    }

    private fun sizeOfByteArray(bytes: ByteArray): ByteArray {
        val buf = bufferPool.take(4)
        buf.putInt(bytes.size)
        val retVal = ByteArray(4)
        System.arraycopy(buf.array(), 0, retVal, 0, 4)
        return retVal
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as Peer

        if (channel != other.channel) return false

        return true
    }

    override fun hashCode(): Int {
        return channel.hashCode()
    }

    override fun close() {
        if (requestingPiece.get() && currentPiece != null) {
            absentPieces.offer(currentPiece!!)
        }
        if (channel.isOpen) channel.close()
        if (pieceBuf != null) bufferPool.give(pieceBuf)
    }

    override fun toString(): String {
        return channel.remoteAddress.toString()
    }

    companion object {
        fun connectAndHandshake(
                address: InetSocketAddress,
                metaInfo: MetaInfo,
                absentPieces: BlockingQueue<PieceInfo>,
                donePieceOfWorks: BlockingQueue<DonePieceOfWork>,
                bufferPool: ByteBufferPool
        ): Peer {
            val channel = SocketChannel.open()
            channel.connect(address)

            val socket = channel.socket()

            val input = socket.getInputStream()
            val output = socket.getOutputStream()

            // parts of handshake
            val pStrLenByte: Byte = 0x13 // the length of the protocol string
            val pStrLen = ByteArray(1) { pStrLenByte }
            val pStr = "BitTorrent protocol".toByteArray() // the protocol string
            val reserved = ByteArray(8) { 0 } // the reserved bytes
            val handshake = pStrLen + pStr + reserved + metaInfo.infoHash + CLIENT_ID.toByteArray()

            output.write(handshake)

            val handshakeLen = 49 + pStr.size
            val buf = ByteBuffer.allocate(handshakeLen)
            while (buf.hasRemaining()) {
                val r = input.read()
                if (r == -1) break
                buf.put(r.toByte())
            }

            // verify the response handshake
            if (!(pStrLen + pStr).contentEquals(buf.array().sliceArray(0 until pStr.size + pStrLen.size))) {
                error("Peer[$address] Handshake Error: invalid protocol (pstrlen or pstr)")
            }
            val metaInfoStartOffset = pStrLen.size + pStr.size + reserved.size
            val metaInfoEndOffset = metaInfoStartOffset + metaInfo.infoHash.size
            if (!metaInfo.infoHash.contentEquals(buf.array().sliceArray(metaInfoStartOffset until metaInfoEndOffset))) {
                error("Peer[$address] Handshake Error: invalid info_hash value")
            }
            // the handshake is considered to be valid (despite the fact that we skipped the reserved bytes and peer id)

            channel.configureBlocking(false)
            assert(channel.isConnected)

            return Peer(channel, absentPieces, donePieceOfWorks, metaInfo, bufferPool)
        }
    }
}
