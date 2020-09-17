package edu.bittorrent.client

import java.nio.ByteBuffer
import java.util.*


interface Message {
    fun id(): MsgId
    fun toByteArrayWithoutLength(): ByteArray

    companion object {
        fun fromBytes(buf: ByteBuffer): Message {
            buf.flip()
            val sizeOfMsg = buf.int
            return when (MsgId.valueOfByte(buf.get())) { // -1
                MsgId.Choke -> Choke()
                MsgId.UnChoke -> UnChoke()
                MsgId.Interested -> Interested()
                MsgId.NotInterested -> NotInterested()
                MsgId.Have -> Have(buf.int)
                MsgId.Bitfield -> {
                    val bitfieldBytes = ByteArray(sizeOfMsg - MSG_ID_SIZE)
                    buf.get(bitfieldBytes)
                    BitField(BitSet.valueOf(bitfieldBytes))
                }
                MsgId.Request -> Request(null)
                MsgId.Piece -> {
                    val pieceIndex = buf.int // -4
                    val begin = buf.int // -4
                    val blockData = ByteArray(sizeOfMsg - U_INT_SIZE * 2 - MSG_ID_SIZE)
                    buf.get(blockData)
                    Piece(BlockPieceInfo(pieceIndex, begin, blockData.size), blockData)
                }
                MsgId.Cancel -> Cancel()
                MsgId.Unknown -> Unknown()
            }
        }
    }
}

enum class MsgId(val value: Byte) {
    Choke(0),
    UnChoke(1),
    Interested(2),
    NotInterested(3),
    Have(4),
    Bitfield(5),
    Request(6),
    Piece(7),
    Cancel(8),
    Unknown(-1);

    companion object {
        fun valueOfByte(value: Byte): MsgId {
            for (id in MsgId.values()) {
                if (value == id.value) {
                    return id
                }
            }
            return Unknown
        }
    }
}

open abstract class NonSerializableMsg : Message {
    override fun toByteArrayWithoutLength(): ByteArray {
        error("Not implemented or not supported")
    }
}

class Choke : NonSerializableMsg() {
    override fun id(): MsgId = MsgId.Choke
}

class NotInterested : NonSerializableMsg() {
    override fun id(): MsgId = MsgId.NotInterested
}

class Have(private val index: Int) : NonSerializableMsg() {
    override fun id(): MsgId = MsgId.Have
    fun pieceIndex(): Int = index
}

class BitField(private val bitSet: BitSet) : NonSerializableMsg() {
    override fun id(): MsgId = MsgId.Bitfield
    fun bitField(): BitSet = bitSet
}

class Piece(private val info: BlockPieceInfo, private val data: ByteArray) : NonSerializableMsg() {
    override fun id(): MsgId = MsgId.Piece
    fun info(): BlockPieceInfo = info
    fun data(): ByteArray = data
}

class Cancel : NonSerializableMsg() {
    override fun id(): MsgId = MsgId.Cancel
}

class Unknown : NonSerializableMsg() {
    override fun id(): MsgId = MsgId.Unknown
}

//////////////////////
// Messages to be sent

class Request(private val info: BlockPieceInfo?) : Message {
    override fun id(): MsgId = MsgId.Request
    override fun toByteArrayWithoutLength(): ByteArray {
        if (info == null) return ByteArray(0)
        val buf = ByteBuffer.allocate(13) // block piece 4 bytes * 3 + 1 byte for id
        buf.put(MsgId.Request.value)
        buf.putInt(info.index)
        buf.putInt(info.begin)
        buf.putInt(info.length)
        return buf.array()
    }
}

class UnChoke : Message {
    override fun id(): MsgId = MsgId.UnChoke
    override fun toByteArrayWithoutLength(): ByteArray = byteArrayOf(MsgId.UnChoke.value)
}

class Interested : Message {
    override fun id(): MsgId = MsgId.Interested
    override fun toByteArrayWithoutLength(): ByteArray = byteArrayOf(MsgId.Interested.value)
}

// (spec) index - integer specifying the zero-based piece index
// (spec) begin - integer specifying the zero-based byte offset within the piece
// (spec) length - integer specifying the requested length
data class BlockPieceInfo(val index: Int, val begin: Int, val length: Int)
