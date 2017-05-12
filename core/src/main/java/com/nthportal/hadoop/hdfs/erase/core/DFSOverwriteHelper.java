package com.nthportal.hadoop.hdfs.erase.core;

import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.net.NetUtils;

import javax.net.SocketFactory;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;

final class DFSOverwriteHelper implements SizedOutputStreamProvider {
    private static final int TIMEOUT_MILLIS = 5000;

    private final String host;
    private final int port;
    private final long length;

    DFSOverwriteHelper(BlockLocation location, int index, long length) throws IOException {
        String[] split = location.getNames()[index].split(":", 2);
        host = split[0];
        port = Integer.parseInt(split[1]);
        this.length = length;
    }

    private SizedOutputStream outputStreamForBlock() throws IOException {
        final Socket socket = SocketFactory.getDefault().createSocket();
        InetSocketAddress address = new InetSocketAddress(host, port);
        NetUtils.connect(socket, address, TIMEOUT_MILLIS);
        socket.setSoTimeout(TIMEOUT_MILLIS);

        OutputStream os = NetUtils.getOutputStream(socket, TIMEOUT_MILLIS);
        return new SizedOutputStream(new BufferedOutputStream(os), length) {
            @Override
            public void close() throws IOException {
                try {
                    socket.close();
                } finally {
                    super.close();
                }
            }
        };
    }

    @Override
    public SizedOutputStream get() throws IOException {
        return outputStreamForBlock();
    }
}
