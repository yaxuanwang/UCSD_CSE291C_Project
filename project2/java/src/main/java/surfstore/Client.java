package surfstore;

import java.io.*;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.Base64;
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
import surfstore.SurfStoreBasic.FileInfo;
import surfstore.SurfStoreBasic.WriteResult;



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

    private static String sha256 (byte[] text) {
        MessageDigest digest = null;
        try {
            digest = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
            System.exit(2);
        }
        byte[] hash = digest.digest(text);
        String encoded = Base64.getEncoder().encodeToString(hash);
        return encoded;
    }

    private static Block stringToBlock (byte[] chunk) {
        Block.Builder builder = Block.newBuilder();
        builder.setData(ByteString.copyFrom(chunk));

        builder.setHash(sha256(chunk));
        logger.info("Block hash is:" + sha256(chunk));
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
            throw new RuntimeException("Exception while splitting the file " + e);
        }

        return blockList;
    }

    private static void writeBlocksToFile (ArrayList<Block> blocks, String filepath) {
        try {
            File localFile = new File(filepath);
            localFile.createNewFile();
            FileOutputStream stream = new FileOutputStream(filepath, false);
            for (Block b: blocks) {
                stream.write(b.getData().toByteArray());
            }
        } catch (IOException e) {
            throw new RuntimeException("Cannot write to local file: " + filepath);
        }
    }


	private void go (String[] args) {
		metadataStub.ping(Empty.newBuilder().build());
        logger.info("Successfully pinged the Metadata server");
        
        blockStub.ping(Empty.newBuilder().build());
        logger.info("Successfully pinged the Blockstore server");
        
        // TODO: Implement your client here
        Namespace c_args = parseArgs(args);
        String operation = c_args.getString("operations");
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
                getVersion(c_args.getString("filename"));
                break;
            default:
                throw new RuntimeException("Invalid operation: " + operation);
        }
	}

    private void upload (String filename) {
        int version = getVersion(filename);

        ArrayList<Block> blocks = fileToBlocks(filename);
        Map<String, Block> tmpMap = new HashMap<>();
        for(Block b: blocks) {
            tmpMap.put(b.getHash(), b);
        }
        FileInfo.Builder builder = FileInfo.newBuilder();

        builder.setFilename(filename);
        builder.setVersion(version+1);
        ArrayList<String> hashlist = new ArrayList<>();
        for (int i=0; i<blocks.size(); i++) {
            hashlist.add(blocks.get(i).getHash());
        }
        builder.addAllBlocklist(hashlist);

        FileInfo request = builder.build();
        WriteResult result = WriteResult.newBuilder(metadataStub.modifyFile(request)).build();
        while (result.getResult() == WriteResult.Result.MISSING_BLOCKS) {
            int count = result.getMissingBlocksCount();
            for (int i=0; i<count; i++) {
                blockStub.storeBlock(tmpMap.get(result.getMissingBlocks(i)));
            }
            result = WriteResult.newBuilder(metadataStub.modifyFile(request)).build();
        }
        if (result.getResult() == WriteResult.Result.OLD_VERSION) {
            upload(filename);
        } else if (result.getResult() == WriteResult.Result.OK) {
            logger.info("Successfully uploaded file: " + filename);
        }

    }

    private void download (String filename, String downloadPath) {
        FileInfo readRequest = FileInfo.newBuilder().setFilename(filename).build();
        FileInfo readResult = metadataStub.readFile(readRequest);
        ArrayList<String> blockHash = new ArrayList<>(readResult.getBlocklistList());
        //TODO: what if there are some missing blocks
        ArrayList<Block> blocks = new ArrayList<>();
        for (String h: blockHash) {
            Block b = blockStub.getBlock(Block.newBuilder().setHash(h).build());
            blocks.add(b);
        }
        writeBlocksToFile(blocks, downloadPath+ "/" + filename);
        logger.info("Successfully downloaded file: " + filename);
    }

    private void delete (String filename) {
        int version = getVersion(filename);
        FileInfo.Builder builder  = FileInfo.newBuilder();
        builder.setFilename(filename);
        builder.setVersion(version+1);

        FileInfo deleteRequest = builder.build();
        WriteResult result = metadataStub.deleteFile(deleteRequest);

        if (result.getResult() == WriteResult.Result.OLD_VERSION) {
            delete(filename);
        } else if (result.getResult() == WriteResult.Result.OK) {
            logger.info("Successfully deleted file: " + filename);
        }

    }

    private int getVersion(String filename) {
        FileInfo fileInfo = FileInfo.newBuilder().setFilename(filename).build();
        int version = metadataStub.readFile(fileInfo).getVersion();
        logger.info("Current version of file " + filename + " is " + version);
        return version;
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
                    && res.getString("downloadPath").equals("")
                    ||!res.getString("operations").equals("download")
                    && !res.getString("downloadPath").equals("") ) {
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
