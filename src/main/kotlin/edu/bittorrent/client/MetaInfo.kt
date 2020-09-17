package edu.bittorrent.client

import com.dampcake.bencode.Bencode
import com.dampcake.bencode.BencodeInputStream
import org.apache.commons.codec.digest.DigestUtils
import org.apache.commons.codec.net.URLCodec
import java.io.ByteArrayInputStream
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets.UTF_8

class MetaInfo(
        bytes: ByteArray
) {

    var announceUrls = emptyList<String>()
    var infoHashEncoded = ""
    var infoHash = ByteArray(0)
    var name = ""
    var length = -1L
    var pieces = emptyList<PieceInfo>()
    var pieceLength = -1L

    init {
        val stream = BencodeInputStream(ByteArrayInputStream(bytes))
        val dict = stream.readDictionary()

        val useBytesDict = BencodeInputStream(ByteArrayInputStream(bytes), UTF_8, true).readDictionary()
        val infoBencoded = Bencode(UTF_8).encode(useBytesDict["info"] as Map<*, *>)
        val infoHashBytes = DigestUtils.sha1(infoBencoded)
        val infoHashEncoded = String(URLCodec().encode(infoHashBytes), UTF_8)

        val info = dict["info"] as Map<String, Object>

        val announce: String = dict["announce"] as String
        val announceList: List<List<String>> = dict.getOrDefault(
                "announce-list",
                emptyList<List<String>>()
        ) as List<List<String>>

        val trackersUrls = mutableListOf<String>(announce)
        trackersUrls.addAll(announceList.flatten())

        val useBytesInfo = useBytesDict["info"] as Map<String, Object>
        val piecesArr = (useBytesInfo["pieces"] as ByteBuffer).array()
        val pieces = piecesArr.asIterable().chunked(20).map { it.toByteArray() }

        this.announceUrls = trackersUrls
        this.infoHashEncoded = infoHashEncoded
        this.infoHash = infoHashBytes
        this.name = info["name"] as String
        this.length = info["length"] as Long
        this.pieces = pieces.mapIndexed { index, hash -> PieceInfo(index, hash) }
        this.pieceLength = info["piece length"] as Long
    }
}

data class PieceInfo(val index: Int, val hash: ByteArray) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as PieceInfo

        if (index != other.index) return false
        if (!hash.contentEquals(other.hash)) return false

        return true
    }

    override fun hashCode(): Int {
        var result = index
        result = 31 * result + hash.contentHashCode()
        return result
    }
}
