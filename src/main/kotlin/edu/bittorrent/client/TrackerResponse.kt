package edu.bittorrent.client

import com.dampcake.bencode.BencodeInputStream
import java.io.ByteArrayInputStream
import java.net.InetAddress
import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

class TrackerResponse(
        private val peers: List<InetSocketAddress>,
        private val failureReason: String?
) {
    fun failureReason(): String? {
        return failureReason
    }

    fun peersAddresses(): List<InetSocketAddress> {
        return peers
    }

    companion object {
        const val FR = "failure reason"

        fun parse(bytes: ByteArray): TrackerResponse {
            val stream = BencodeInputStream(ByteArrayInputStream(bytes))
            val dict = stream.readDictionary()

            if (dict[FR] != null && dict[FR] is String) {
                return TrackerResponse(mutableListOf(), dict[FR] as String)
            }

            val peers: MutableList<InetSocketAddress> = mutableListOf()
            val peersBencode = dict["peers"]
            if (peersBencode is List<*>) {
                peersBencode.forEach { peer ->
                    if (peer is Map<*, *>) {
                        val host = peer["ip"] as String
                        val port = peer["port"] as Int
                        peers.add(InetSocketAddress(host, port))
                    }
                }
            } else {
                // peers compact format
                stream.close()
                val bstream = BencodeInputStream(ByteArrayInputStream(bytes), StandardCharsets.UTF_8, true)
                val dict = bstream.readDictionary()
                val peersBytes = dict["peers"] as ByteBuffer
                peers.addAll(parseBinaryPeers(peersBytes))
            }

            return TrackerResponse(
                    peers, null
            )
        }

        private fun parseBinaryPeers(data: ByteBuffer): List<InetSocketAddress> {
            val size = data.array().size
            if (size % 6 != 0) {
                throw RuntimeException("Invalid peers binary information string!")
            }

            val peers = mutableListOf<InetSocketAddress>()
            for (i in 0 until size / 6) {
                val ipBytes = ByteArray(4)
                data.get(ipBytes)
                val ip = InetAddress.getByAddress(ipBytes)
                val a = data.get()
                val b = data.get()
                val port = ((0xFF and a.toInt()) shl 8) or (0xFF and b.toInt())
                peers.add(InetSocketAddress(ip, port))
            }

            return peers
        }
    }
}

