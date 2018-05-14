package surfstore;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;


import com.google.protobuf.ByteString;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import surfstore.SurfStoreBasic.Empty;
import surfstore.SurfStoreBasic.Block;
import surfstore.SurfStoreBasic.Block.*;

import javax.imageio.IIOException;


public final class Client {
    private static final Logger logger = Logger.getLogger(Client.class.getName());

    private final ManagedChannel metadataChannel;
    private final MetadataStoreGrpc.MetadataStoreBlockingStub metadataStub;

    private final ManagedChannel blockChannel;
    private final BlockStoreGrpc.BlockStoreBlockingStub blockStub;

    private final ConfigReader config;

    public Client(ConfigReader config) {
        this.metadataChannel = ManagedChannelBuilder.forAddress("127.0.0.1", config.getMetadataPort(1))
                .usePlaintext(true).build();
        this.metadataStub = MetadataStoreGrpc.newBlockingStub(metadataChannel);

        this.blockChannel = ManagedChannelBuilder.forAddress("127.0.0.1", config.getBlockPort())
                .usePlaintext(true).build();
        this.blockStub = BlockStoreGrpc.newBlockingStub(blockChannel);

        this.config = config;
    }

    public void shutdown() throws InterruptedException {
        metadataChannel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
        blockChannel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }

    private void ensure(boolean b) {
        if (b == false) {
            throw new RuntimeException("Assertion Failed!");
        }
    }

    private static Block stringToBlock (byte[] chunk) {
        Builder builder = Block.newBuilder();
        builder.setData(ByteString.copyFrom(chunk));

        builder.setHash(HashUtils.sha256(chunk));
        return builder.build();
    }

    // split file into chunks and create Blocks
    private static ArrayList<Block> fileToBlocks (String fileName) {
        ArrayList<Block> blockList = new ArrayList<>();
        int PART_SIZE = 4 * 1024;
        byte[] buffer = new byte[PART_SIZE];
        File f = new File(fileName);

        try (FileInputStream fis = new FileInputStream(f);
             BufferedInputStream bis = new BufferedInputStream(fis)) {

            int bytesAmount = 0;
            while ((bytesAmount = bis.read(buffer)) > 0) {
                if (bytesAmount == PART_SIZE) {
                    Block b = stringToBlock(buffer);
                    if (b != null) {
                        blockList.add(b);
                    }
                } else {
                    byte[] lastBuffer = Arrays.copyOf(buffer, bytesAmount);
                    Block b = stringToBlock(lastBuffer);
                    if (b != null) {
                        blockList.add(b);
                    }
                }
            }
        } catch (IOException e) {
            System.out.println("Exception while splitting the file " + e);
        }

        return blockList;
    }

	private void go(String[] args) {
//		metadataStub.ping(Empty.newBuilder().build());
//        logger.info("Successfully pinged the Metadata server");
        
        blockStub.ping(Empty.newBuilder().build());
        logger.info("Successfully pinged the Blockstore server");
        
        // TODO: Implement your client here
        Namespace c_args = parseArgs(args);
        String operation = c_args.getString("operations");
        int answer = 0;
        switch(operation) {
            case "upload":
                upload(c_args.getString("filename"));
                break;
            case "download":
                download(c_args.getString("filename"), c_args.getString("downloadPath"));
                break;
            case "delete":
                delete(c_args.getString("filename"));
                break;
            case "getversion":
                answer = getVersion(c_args.getString("filename"));
                break;
            default:
                throw new RuntimeException("Invalid operation: " + operation);
        }
//        Block b1 = stringToBlock("block_01");
//        Block b2 = stringToBlock("block_02");
//
//        ensure(blockStub.hasBlock(b1).getAnswer() == false);
//        ensure(blockStub.hasBlock(b2).getAnswer() == false);
//
//        blockStub.storeBlock(b1);
//        ensure(blockStub.hasBlock(b1).getAnswer() == true);
//
//        blockStub.storeBlock(b2);
//        ensure(blockStub.hasBlock(b2).getAnswer() == true);
//
//        Block b1prime = blockStub.getBlock(b1);
//        ensure(b1prime.getHash().equals(b1.getHash()));
//        ensure(b1prime.getData().equals(b1.getData()));
//
//        logger.info("We passed all the tests!");
	}

    private void upload(String filename) {

    }

    private void download(String filename, String downloadPath) {

    }

    private void delete(String filename) {

    }

    private int getVersion(String filename) {
        return 0;
    }

	/*
	 * TODO: Add command line handling here
	 */
    private static Namespace parseArgs(String[] args) {
        ArgumentParser parser = ArgumentParsers.newFor("Client").build()
                .description("Client for SurfStore");
        parser.addArgument("config_file").type(String.class)
                .help("Path to configuration file");

        parser.addArgument("operations").choices("upload", "download", "delete", "getversion")
                .type(String.class)
                .help("file operations");

        parser.addArgument("filename").type(String.class)
                .help("file name or file path");

        parser.addArgument("downloadPath").nargs("?")
                .setDefault("");
        
        Namespace res = null;
        try {
            res = parser.parseArgs(args);
            if (res.getString("operations").equals("download")
                    && res.getString("downloadPath").equals("")) {
                throw new RuntimeException("Argument parsing failed");
            }
        } catch (ArgumentParserException e){
            parser.handleError(e);
        }
        return res;
    }

    public static void main(String[] args) throws Exception {
        Namespace c_args = parseArgs(args);
        if (c_args == null){
            throw new RuntimeException("Argument parsing failed");
        }
        System.out.println(c_args);

        File configf = new File(c_args.getString("config_file"));
        ConfigReader config = new ConfigReader(configf);

        Client client = new Client(config);

        try {
        	client.go(args);
        } finally {
            client.shutdown();
        }
    }

}
