package io.unbong.ubmq.store;

import java.io.File;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

/**
 * mmap store
 *
 * @author <a href="ecunbong@gmail.com">unbong</a>
 * 2024-07-14 15:53
 */
public class StoreDemo {

    public static void main(String[] args) throws IOException {
        String content = """
                    this is good file.
                    that is new line.
                """;

        File file = new File("test.dat");
        if(!file.exists()) {
            file.createNewFile();
        }

        Path path = Paths.get(file.getAbsolutePath());
        try(FileChannel ch = (FileChannel) Files.newByteChannel(
                path, StandardOpenOption.READ, StandardOpenOption.WRITE
        )){
            MappedByteBuffer byteBuffer = ch.map(FileChannel.MapMode.READ_WRITE,0,1024);
            for (int i = 0; i < 10; i++) {
                byteBuffer.put(Charset.forName("UTF-8").encode(content));
            }
        }
    }
}
