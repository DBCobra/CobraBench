package kv_interfaces.instrument;

import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.SecureRandom;
import java.security.Signature;
import java.security.SignatureException;
import java.util.Base64;
import java.util.Base64.Decoder;
import java.util.Base64.Encoder;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class SignatureManager extends Thread {

	private Signature dsa = null;
	private Encoder encoder;
	private AtomicInteger nextReceipt;
	private BlockingQueue<Work> workToDo;
	private ConcurrentHashMap<Integer, String> results;

	private static SignatureManager instance = null;

	public static SignatureManager getInstance() {
		if (instance == null) {
			synchronized (SignatureManager.class) {
				if (instance == null) {
					instance = new SignatureManager();
				}
			}
		}
		return instance;
	}

	private SignatureManager() {
		try {
			workToDo = new LinkedBlockingQueue<SignatureManager.Work>();
			nextReceipt = new AtomicInteger(0);
			results = new ConcurrentHashMap<Integer, String>();

			KeyPairGenerator keyGen = KeyPairGenerator.getInstance("EC");
			SecureRandom random = SecureRandom.getInstance("SHA1PRNG");
			keyGen.initialize(256, random);
			KeyPair pair = keyGen.generateKeyPair();

			dsa = Signature.getInstance("SHA256withECDSA");
			dsa.initSign(pair.getPrivate());
			encoder = Base64.getEncoder();

			System.out.println("your PRIVATE KEY is: " + encoder.encodeToString(pair.getPrivate().getEncoded()));
			System.out.println("your public key is: " + encoder.encodeToString(pair.getPublic().getEncoded()));
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(-1);
		}
	}

	/**
	 * We just receive your money, and give you a receipt. Don't worry, we will
	 * process your request sooner or later. So just go home to wait and remember do
	 * not lose your receipt.
	 * 
	 * @param input
	 * @return an Integer that you can use to retrieve your signature.
	 * @throws SignatureException
	 */
	public int requestSign(byte[] input) {
		int receipt = nextReceipt.getAndIncrement();
		Work work = new Work(receipt, input);
		workToDo.add(work);
		return receipt;
	}

	/**
	 * After you are given a receipt, you can use that receipt to retrieve the
	 * signature result.
	 * 
	 * @param receipt
	 * @return The signature String related to the receipt Id.
	 */
	public String getSignature(int receipt) {
		assert receipt < nextReceipt.get();
		return results.remove(receipt);
//		return results.get(receipt);
	}

	private void doOneWork(Work work) throws SignatureException {
		dsa.update(work.data);
		byte[] realSign = dsa.sign();
		String output = encoder.encodeToString(realSign);
		this.results.put(work.receipt, output);
	}

	@Override
	public void run() {
		System.out.println("SignatureManager starts");
		for (;;) {
			try {
				doOneWork(workToDo.take());
			} catch (SignatureException e) {
				e.printStackTrace();
				System.exit(-1);
			} catch (InterruptedException e) {
				e.printStackTrace();
				System.exit(-1);
			}
		}
	}

	private class Work {
		public int receipt;
		public byte[] data;

		public Work(int receipt, byte[] data) {
			this.receipt = receipt;
			this.data = data;
		}
	}

	public static void __how_to_use_ECDSA() throws Exception {
//	public static void main(String args[]) throws Exception{
		KeyPairGenerator keyGen = KeyPairGenerator.getInstance("EC");
		SecureRandom random = SecureRandom.getInstance("SHA1PRNG");

		keyGen.initialize(256, random);

		KeyPair pair = keyGen.generateKeyPair();
		PrivateKey priv = pair.getPrivate();
		PublicKey pub = pair.getPublic();

		Signature dsa = Signature.getInstance("SHA256withECDSA");
		dsa.initSign(priv);

		Signature dsa1 = Signature.getInstance("SHA256withECDSA");
		dsa1.initVerify(pub);

		String str = "This is a string to sign and this is very loooooooo"
				+ "oooooooooooooooooooooooooooooooooooooooooooooooooooong";

		Encoder encoder = Base64.getEncoder();
		Decoder decoder = Base64.getDecoder();

		long t1 = System.currentTimeMillis();
		for (int i = 0; i < 1000; i++) {
			byte[] strByte = str.getBytes("UTF-8");
			dsa.update(strByte);
			byte[] realSig = dsa.sign();
			String sigStr = encoder.encodeToString(realSig);
//			System.out.println("Signature: " + sigStr + ", length: " + sigStr.length());
			dsa1.update(strByte);
			byte[] decodedSig = decoder.decode(sigStr);
			boolean result = dsa1.verify(decodedSig);
//			System.out.println(result);
		}
		long t2 = System.currentTimeMillis();

		System.out.println("Runtime of 1000 rounds: " + (t2 - t1) + "ms.");
	}

	public static void main(String[] args) {
		SignatureManager sm = SignatureManager.getInstance();
		sm.start();

		String msg = "this is a message";
		byte[] msgBytes = msg.getBytes();
		int receipt = sm.requestSign(msgBytes);
		System.out.println("got receipt: " + receipt);
		String sign = null;
		long t1 = System.currentTimeMillis();
		while ((sign = sm.getSignature(receipt)) == null) {
			System.out.println("waiting");
		}
		long t2 = System.currentTimeMillis();
		System.out.println(sign);
		System.out.println("signature length: " + sign.length());
		System.out.println(t2 - t1 + "ms");
	}

}
