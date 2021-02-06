package exercise_1;

import org.apache.tika.language.LanguageIdentifier;
import org.apache.tika.language.LanguageProfile;

public class LanguageDetector {

	static boolean isEnglish(String s) {
		LanguageIdentifier identifier = new LanguageIdentifier(s);
		return identifier.getLanguage().equals("en");
	}
	
}
