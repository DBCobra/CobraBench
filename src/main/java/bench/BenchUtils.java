package bench;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import main.Config;

public class BenchUtils {
	
	// string generator
	public static final String upper = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
	public static final String lower = upper.toLowerCase();
	public static final String digits = "0123456789";
	public static final String alphanum = upper + lower + digits;

	public static String getRandomValue() {
		String res = getRandomValue(Config.get().VALUE_LENGTH);
		return res;
	}

	public static String getRandomValue(int length) {
		int size = alphanum.length();
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < length; i++) {
			int pos = ThreadLocalRandom.current().nextInt(size);
			sb.append(alphanum.charAt(pos));
		}
		return sb.toString();
	}
	
    public static String shuffleString(String input){
        List<Character> characters = new ArrayList<Character>();
        for(char c:input.toCharArray()){
            characters.add(c);
        }
        StringBuilder output = new StringBuilder(input.length());
        while(characters.size()!=0){
            int randPicker = (int)(Math.random()*characters.size());
            output.append(characters.remove(randPicker));
        }
        return output.toString();
    }
	
	// integer generator
	public static int getRandomInt(int from, int to) {
		assert to > from;
		int rand = ThreadLocalRandom.current().nextInt(to - from);
		return from + rand;
	}
	
	public static String MakeTimeStamp() {
		return new SimpleDateFormat("dd-MM-yyyy").format(new Date());
	}
	
	public static boolean getBitMapAt(byte[] b, int p) {
        return (b[p/8] & (1<<(p%8))) != 0;
    }
	
	public static void setBitMapAt(byte[] b, int p) {
		b[p/8] |= 1<<(p%8);
	}
	
	public static void clearBitMapAt(byte[] b, int p) {
		b[p/8] &= ~(1<<(p%8));
	}
}
