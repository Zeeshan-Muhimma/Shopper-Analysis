import azure.functions as func
import logging
import pandas as pd
import nltk
from nltk.tokenize import word_tokenize
from io import BytesIO
import io
from azure.data.tables import TableServiceClient
import re
from collections import defaultdict
import re
from tqdm import tqdm

app = func.FunctionApp()

@app.blob_trigger(arg_name="myblob", path="gpt-reviews/{name}",
                               connection="muhimmablob_STORAGE") 
def blob_trigger(myblob: func.InputStream):
    logging.info(f"Python blob trigger function processed blob"
                f"Name: {myblob.name}"
                f"Blob Size: {myblob.length} bytes")
    # Read the blob content
    blob_bytes = myblob.read()
    blob_stream = BytesIO(blob_bytes)
    blob_stream.seek(0) 
    nltk.download('punkt')
    df = pd.read_excel(blob_stream, index_col=None, header=0,engine='openpyxl') 
    english_stopwords = [",","0o", "0s", "3a", "3b", "3d", "6b", "6o", "a", "a1", "a2", "a3",\
    "a4", "ab", "able", "about", "above", "abst", "ac", "accordance", "according", \
    "accordingly", "across", "act", "actually", "ad", "added", "adj", "ae", "af", \
    "affected", "affecting", "affects", "after", "afterwards", "ag", "again", "against", \
    "ah", "ain", "ain't", "aj", "al", "all", "allow", "allows", "almost", "alone", "along", "already", "also", "although", "always", "am", "among", "amongst", "amoungst", "amount", "an", "and", "announce", "another", "any", "anybody", "anyhow", "anymore", "anyone", "anything", "anyway", "anyways", "anywhere", "ao", "ap", "apart", "apparently", "appear", "appreciate", "appropriate", "approximately", "ar", "are", "aren", "arent", "aren't", "arise", "around", "as", "a's", "aside", "ask", "asking", "associated", "at", "au", "auth", "av", "available", "aw", "away", "awfully", "ax", "ay", "az", "b", "b1", "b2", "b3", "ba", "back", "bc", "bd", "be", "became", "because", "become", "becomes", "becoming", "been", "before", "beforehand", "begin", "beginning", "beginnings", "begins", "behind", "being", "believe", "below", "beside", "besides", "best", "better", "between", "beyond", "bi", "bill", "biol", "bj", "bk", "bl", "bn", "both", "bottom", "bp", "br", "brief", "briefly", "bs", "bt", "bu", "but", "bx", "by", "c", "c1", "c2", "c3", "ca", "call", "came", "can", "cannot", "cant", "can't", "cause", "causes", "cc", "cd", "ce", "certain", "certainly", "cf", "cg", "ch", "changes", "ci", "cit", "cj", "cl", "clearly", "cm", "c'mon", "cn", "co", "com", "come", "comes", "con", "concerning", "consequently", "consider", "considering", "contain", "containing", "contains", "corresponding", "could", "couldn", "couldnt", "couldn't", "course", "cp", "cq", "cr", "cry", "cs", "c's", "ct", "cu", "currently", "cv", "cx", "cy", "cz", "d", "d2", "da", "date", "dc", "dd", "de", "definitely", "describe", "described", "despite", "detail", "df", "di", "did", "didn", "didn't", "different", "dj", "dk", "dl", "do", "does", "doesn", "doesn't", "doing", "don", "done", "don't", "down", "downwards", "dp", "dr", "ds", "dt", "du", "due", "during", "dx", "dy", "e", "e2", "e3", "ea", "each", "ec", "ed", "edu", "ee", "ef", "effect", "eg", "ei", "eight", "eighty", "either", "ej", "el", "eleven", "else", "elsewhere", "em", "empty", "en", "end", "ending", "enough", "entirely", "eo", "ep", "eq", "er", "es", "especially", "est", "et", "et-al", "etc", "eu", "ev", "even", "ever", "every", "everybody", "everyone", "everything", "everywhere", "ex", "exactly", "example", "except", "ey", "f", "f2", "fa", "far", "fc", "few", "ff", "fi", "fifteen", "fifth", "fify", "fill", "find", "fire", "first", "five", "fix", "fj", "fl", "fn", "fo", "followed", "following", "follows", "for", "former", "formerly", "forth", "forty", "found", "four", "fr", "from", "front", "fs", "ft", "fu", "full", "further", "furthermore", "fy", "g", "ga", "gave", "ge", "get", "gets", "getting", "gi", "give", "given", "gives", "giving", "gj", "gl", "go", "goes", "going", "gone", "got", "gotten", "gr", "greetings", "gs", "gy", "h", "h2", "h3", "had", "hadn", "hadn't", "happens", "hardly", "has", "hasn", "hasnt", "hasn't", "have", "haven", "haven't", "having", "he", "hed", "he'd", "he'll", "hello", "help", "hence", "her", "here", "hereafter", "hereby", "herein", "heres", "here's", "hereupon", "hers", "herself", "hes", "he's", "hh", "hi", "hid", "him", "himself", "his", "hither", "hj", "ho", "home", "hopefully", "how", "howbeit", "however", "how's", "hr", "hs", "http", "hu", "hundred", "hy", "i", "i2", "i3", "i4", "i6", "i7", "i8", "ia", "ib", "ibid", "ic", "id", "i'd", "ie", "if", "ig", "ignored", "ih", "ii", "ij", "il", "i'll", "im", "i'm", "immediate", "immediately", "importance", "important", "in", "inasmuch", "inc", "indeed", "index", "indicate", "indicated", "indicates", "information", "inner", "insofar", "instead", "interest", "into", "invention", "inward", "io", "ip", "iq", "ir", "is", "isn", "isn't", "it", "itd", "it'd", "it'll", "its", "it's", "itself", "iv", "i've", "ix", "iy", "iz", "j", "jj", "jr", "js", "jt", "ju", "just", "k", "ke", "keep", "keeps", "kept", "kg", "kj", "km", "know", "known", "knows", "ko", "l", "l2", "la", "largely", "last", "lately", "later", "latter", "latterly", "lb", "lc", "le", "least", "les", "less", "lest", "let", "lets", "let's", "lf", "like", "liked", "likely", "line", "little", "lj", "ll", "ll", "ln", "lo", "look", "looking", "looks", "los", "lr", "ls", "lt", "ltd", "m", "m2", "ma", "made", "mainly", "make", "makes", "many", "may", "maybe", "me", "mean", "means", "meantime", "meanwhile", "merely", "mg", "might", "mightn", "mightn't", "mill", "million", "mine", "miss", "ml", "mn", "mo", "more", "moreover", "most", "mostly", "move", "mr", "mrs", "ms", "mt", "mu", "much", "mug", "must", "mustn", "mustn't", "my", "myself", "n", "n2", "na", "name", "namely", "nay", "nc", "nd", "ne", "near", "nearly", "necessarily", "necessary", "need", "needn", "needn't", "needs", "neither", "never", "nevertheless", "new", "next", "ng", "ni", "nine", "ninety", "nj", "nl", "nn", "no", "nobody", "non", "none", "nonetheless", "noone", "nor", "normally", "nos", "not", "noted", "nothing", "novel", "now", "nowhere", "nr", "ns", "nt", "ny", "o", "oa", "ob", "obtain", "obtained", "obviously", "oc", "od", "of", "off", "often", "og", "oh", "oi", "oj", "ok", "okay", "ol", "old", "om", "omitted", "on", "once", "one", "ones", "only", "onto", "oo", "op", "oq", "or", "ord", "os", "ot", "other", "others", "otherwise", "ou", "ought", "our", "ours", "ourselves", "out", "outside", "over", "overall", "ow", "owing", "own", "ox", "oz", "p", "p1", "p2", "p3", "page", "pagecount", "pages", "par", "part", "particular", "particularly", "pas", "past", "pc", "pd", "pe", "per", "perhaps", "pf", "ph", "pi", "pj", "pk", "pl", "placed", "please", "plus", "pm", "pn", "po", "poorly", "possible", "possibly", "potentially", "pp", "pq", "pr", "predominantly", "present", "presumably", "previously", "primarily", "probably", "promptly", "proud", "provides", "ps", "pt", "pu", "put", "py", "q", "qj", "qu", "que", "quickly", "quite", "qv", "r", "r2", "ra", "ran", "rather", "rc", "rd", "re", "readily", "really", "reasonably", "recent", "recently", "ref", "refs", "regarding", "regardless", "regards", "related", "relatively", "research", "research-articl", "respectively", "resulted", "resulting", "results", "rf", "rh", "ri", "right", "rj", "rl", "rm", "rn", "ro", "rq", "rr", "rs", "rt", "ru", "run", "rv", "ry", "s", "s2", "sa", "said", "same", "saw", "say", "saying", "says", "sc", "sd", "se", "sec", "second", "secondly", "section", "see", "seeing", "seem", "seemed", "seeming", "seems", "seen", "self", "selves", "sensible", "sent", "serious", "seriously", "seven", "several", "sf", "shall", "shan", "shan't", "she", "shed", "she'd", "she'll", "shes", "she's", "should", "shouldn", "shouldn't", "should've", "show", "showed", "shown", "showns", "shows", "si", "side", "significant", "significantly", "similar", "similarly", "since", "sincere", "six", "sixty", "sj", "sl", "slightly", "sm", "sn", "so", "some", "somebody", "somehow", "someone", "somethan", "something", "sometime", "sometimes", "somewhat", "somewhere", "soon", "sorry", "sp", "specifically", "specified", "specify", "specifying", "sq", "sr", "ss", "st", "still", "stop", "strongly", "sub", "substantially", "successfully", "such", "sufficiently", "suggest", "sup", "sure", "sy", "system", "sz", "t", "t1", "t2", "t3", "take", "taken", "taking", "tb", "tc", "td", "te", "tell", "ten", "tends", "tf", "th", "than", "thank", "thanks", "thanx", "that", "that'll", "thats", "that's", "that've", "the", "their", "theirs", "them", "themselves", "then", "thence", "there", "thereafter", "thereby", "thered", "therefore", "therein", "there'll", "thereof", "therere", "theres", "there's", "thereto", "thereupon", "there've", "these", "they", "theyd", "they'd", "they'll", "theyre", "they're", "they've", "thickv", "thin", "think", "third", "this", "thorough", "thoroughly", "those", "thou", "though", "thoughh", "thousand", "three", "throug", "through", "throughout", "thru", "thus", "ti", "til", "tip", "tj", "tl", "tm", "tn", "to", "together", "too", "took", "top", "toward", "towards", "tp", "tq", "tr", "tried", "tries", "truly", "try", "trying", "ts", "t's", "tt", "tv", "twelve", "twenty", "twice", "two", "tx", "u", "u201d", "ue", "ui", "uj", "uk", "um", "un", "under", "unfortunately", "unless", "unlike", "unlikely", "until", "unto", "uo", "up", "upon", "ups", "ur", "us", "use", "used", "useful", "usefully", "usefulness", "uses", "using", "usually", "ut", "v", "va", "value", "various", "vd", "ve", "ve", "very", "via", "viz", "vj", "vo", "vol", "vols", "volumtype", "vq", "vs", "vt", "vu", "w", "wa", "want", "wants", "was", "wasn", "wasnt", "wasn't", "way", "we", "wed", "we'd", "welcome", "well", "we'll", "well-b", "went", "were", "we're", "weren", "werent", "weren't", "we've", "what", "whatever", "what'll", "whats", "what's", "when", "whence", "whenever", "when's", "where", "whereafter", "whereas", "whereby", "wherein", "wheres", "where's", "whereupon", "wherever", "whether", "which", "while", "whim", "whither", "who", "whod", "whoever", "whole", "who'll", "whom", "whomever", "whos", "who's", "whose", "why", "why's", "wi", "widely", "will", "willing", "wish", "with", "within", "without", "wo", "won", "wonder", "wont", "won't", "words", "world", "would", "wouldn", "wouldnt", "wouldn't", "www", "x", "x1", "x2", "x3", "xf", "xi", "xj", "xk", "xl", "xn", "xo", "xs", "xt", "xv", "xx", "y", "y2", "yes", "yet", "yj", "yl", "you", "youd", "you'd", "you'll", "your", "youre", "you're", "yours", "yourself", "yourselves", "you've", "yr", "ys", "yt", "z", "zero", "zi", "zz",]
    # Assuming you have a list of Arabic stopwords
    arabic_stopwords = ['،', 'ـ', 'ء', 'ءَ', 'آ', 'أ', 'ا', 'ا?', 'االا', 'االتى', 'آب', 'أبٌ', 'ابتدأ', 'أبدا', 'أبريل', 'أبو', 'ابين', 'اتخذ', 'اثر', 'اثنا', 'اثنان', 'اثني', 'اثنين', 'أجل', 'اجل', 'أجمع', 'أحد', 'احد', 'إحدى', 'أخٌ', 'أخبر', 'أخذ', 'آخر', 'اخرى', 'اخلولق', 'أخو', 'إذ', 'إذا', 'إذاً', 'اذا', 'آذار', 'إذما', 'إذن', 'أربع', 'أربعاء', 'أربعة', 'اربعة', 'أربعمائة', 'أربعمئة', 'اربعون', 'اربعين', 'ارتدّ', 'أرى', 'إزاء', 'استحال', 'أسكن', 'أصبح', 'اصبح', 'أصلا', 'آض', 'إضافي', 'أضحى', 'اضحى', 'اطار', 'أطعم', 'اعادة', 'أعطى', 'أعلم', 'اعلنت', 'أغسطس', 'أُفٍّ', 'أفٍّ', 'اف', 'أفريل', 'أفعل به', 'أقبل', 'أكتوبر', 'أكثر', 'اكثر', 'اكد', 'آل', 'أل', 'ألا', 'إلا', 'إلّا', 'الا', 'الاخيرة', 'الألاء', 'الألى', 'الآن', 'الان', 'الاول', 'الاولى', 'التي', 'التى', 'الثاني', 'الثانية', 'الحالي', 'الذاتي', 'الذي', 'الذى', 'الذين', 'السابق', 'ألف', 'الف', 'ألفى', 'اللاتي', 'اللتان', 'اللتيا', 'اللتين', 'اللذان', 'اللذين', 'اللواتي', 'الماضي', 'المقبل', 'الوقت', 'إلي', 'إلى', 'الي', 'الى', 'إلَيْكَ', 'إليكَ', 'إليكم', 'إليكما', 'إليكنّ', 'اليه', 'اليها', 'اليوم', 'أم', 'أما', 'أمّا', 'إما', 'إمّا', 'اما', 'أمام', 'امام', 'أمامك', 'أمامكَ', 'أمد', 'أمس', 'امس', 'أمسى', 'امسى', 'آمينَ', 'أن', 'أنًّ', 'إن', 'إنَّ', 'ان', 'أنا', 'آناء', 'أنبأ', 'انبرى', 'أنت', 'أنتِ', 'انت', 'أنتم', 'أنتما', 'أنتن', 'أنشأ', 'آنفا', 'أنفسكم', 'أنفسنا', 'أنفسهم', 'انقلب', 'أنه', 'إنه', 'انه', 'أنها', 'إنها', 'انها', 'أنّى', 'آه', 'آهٍ', 'آهِ', 'آهاً', 'أهلا', 'أو', 'او', 'أوت', 'أوشك', 'أول', 'اول', 'أولاء', 'أولالك', 'أولئك', 'أوّهْ', 'أي', 'أيّ', 'أى', 'إى', 'اي', 'اى', 'ا?ى', 'أيا', 'أيار', 'ايار', 'إياك', 'إياكم', 'إياكما', 'إياكن', 'ايام', 'ّأيّان', 'أيّان', 'إيانا', 'إياه', 'إياها', 'إياهم', 'إياهما', 'إياهن', 'إياي', 'أيضا', 'ايضا', 'أيلول', 'أين', 'إيهٍ', 'ب', 'باء', 'بات', 'باسم', 'بأن', 'بإن', 'بان', 'بخٍ', 'بد', 'بدلا', 'برس', 'بَسْ', 'بسّ', 'بسبب', 'بشكل', 'بضع', 'بطآن', 'بعد', 'بعدا', 'بعض', 'بعيدا', 'بغتة', 'بل', 'بَلْهَ', 'بلى', 'بن', 'به', 'بها', 'بهذا', 'بؤسا', 'بئس', 'بيد', 'بين', 'بينما', 'ة', 'ت', 'تاء', 'تارة', 'تاسع', 'تانِ', 'تانِك', 'تبدّل', 'تجاه', 'تحت', 'تحت\'', 'تحوّل', 'تخذ', 'ترك', 'تسع', 'تسعة', 'تسعمائة', 'تسعمئة', 'تسعون', 'تسعين', 'تشرين', 'تعسا', 'تعلَّم', 'تفعلان', 'تفعلون', 'تفعلين', 'تكون', 'تلقاء', 'تلك', 'تم', 'تموز', 'تِه', 'تِي', 'تَيْنِ', 'تينك', 'ث', 'ثاء', 'ثالث', 'ثامن', 'ثان', 'ثاني', 'ثانية', 'ثلاث', 'ثلاثاء', 'ثلاثة', 'ثلاثمائة', 'ثلاثمئة', 'ثلاثون', 'ثلاثين', 'ثم', 'ثمَّ', 'ثمّ', 'ثمان', 'ثمانمئة', 'ثمانون', 'ثماني', 'ثمانية', 'ثمانين', 'ثمّة', 'ثمنمئة', 'ج', 'جانفي', 'جدا', 'جعل', 'جلل', 'جمعة', 'جميع', 'جنيه', 'جوان', 'جويلية', 'جير', 'جيم', 'ح', 'حاء', 'حادي', 'حار', 'حاشا', 'حاليا', 'حاي', 'حبذا', 'حبيب', 'حتى', 'حجا', 'حدَث', 'حَذارِ', 'حرى', 'حزيران', 'حسب', 'حقا', 'حمٌ', 'حمدا', 'حمو', 'حوالى', 'حول', 'حيَّ', 'حيث', 'حيثما', 'حين', 'خ', 'خاء', 'خارج', 'خاصة', 'خال', 'خامس', 'خبَّر', 'خلا', 'خلافا', 'خلال', 'خلف', 'خمس', 'خمسة', 'خمسمائة', 'خمسمئة', 'خمسون', 'خمسين', 'خميس', 'د', 'دال', 'درهم', 'درى', 'دواليك', 'دولار', 'دون', 'دونك', 'ديسمبر', 'ديك', 'دينار', 'ذ', 'ذا', 'ذات', 'ذاك', 'ذال', 'ذانِ', 'ذانك', 'ذلك', 'ذِه', 'ذهب', 'ذو', 'ذِي', 'ذيت', 'ذَيْنِ', 'ذينك', 'ر', 'راء', 'رابع', 'راح', 'رأى', 'رُبَّ', 'رجع', 'رزق', 'رويدك', 'ريال', 'ريث', 'ز', 'زاي', 'زعم', 'زود', 'زيارة', 'س', 'ساء', 'سابع', 'سادس', 'سبت', 'سبتمبر', 'سبحان', 'سبع', 'سبعة', 'سبعمائة', 'سبعمئة', 'سبعون', 'سبعين', 'ست', 'ستة', 'ستكون', 'ستمائة', 'ستمئة', 'ستون', 'ستين', 'سحقا', 'سرا', 'سرعان', 'سقى', 'سمعا', 'سنة', 'سنتيم', 'سنوات', 'سوف', 'سوى', 'سين', 'ش', 'شباط', 'شبه', 'شَتَّانَ', 'شتانَ', 'شخصا', 'شرع', 'شمال', 'شيكل', 'شين', 'ص', 'صاد', 'صار', 'صباح', 'صباحا', 'صبر', 'صبرا', 'صدقا', 'صراحة', 'صفر', 'صهٍ', 'صهْ', 'ض', 'ضاد', 'ضحوة', 'ضد', 'ضمن', 'ط', 'طاء', 'طاق', 'طالما', 'طرا', 'طفق', 'طَق', 'ظ', 'ظاء', 'ظل', 'ظلّ', 'ظنَّ', 'ع', 'عاد', 'عاشر', 'عام', 'عاما', 'عامة', 'عجبا', 'عدَّ', 'عدا', 'عدة', 'عدد', 'عَدَسْ', 'عدم', 'عسى', 'عشر', 'عشرة', 'عشرون', 'عشرين', 'عل', 'علًّ', 'علق', 'علم', 'علي', 'على', 'عليك', 'عليه', 'عليها', 'عن', 'عند', 'عندما', 'عنه', 'عنها', 'عوض', 'عيانا', 'عين', 'غ', 'غادر', 'غالبا', 'غدا', 'غداة', 'غير', 'غين', 'ف', 'فاء', 'فأن', 'فإن', 'فان', 'فانه', 'فبراير', 'فرادى', 'فضلا', 'فعل', 'فقد', 'فقط', 'فكان', 'فلان', 'فلس', 'فما', 'فهو', 'فهي', 'فهى', 'فو', 'فوق', 'في', 'فى', 'فيفري', 'فيه', 'فيها', 'ق', 'قاطبة', 'قاف', 'قال', 'قام', 'قبل', 'قد', 'قرش', 'قطّ', 'قلما', 'قليل', 'قوة', 'ك', 'كاد', 'كاف', 'كأن', 'كأنّ', 'كان', 'كانت', 'كانون', 'كأيّ', 'كأيّن', 'كثيرا', 'كِخ', 'كذا', 'كذلك', 'كرب', 'كسا', 'كل', 'كلا', 'كلَّا', 'كلتا', 'كلم', 'كلّما', 'كم', 'كما', 'كن', 'كى', 'كيت', 'كيف', 'كيفما', 'ل', 'لا', 'لات', 'لازال', 'لاسيما', 'لا سيما', 'لام', 'لأن', 'لايزال', 'لبيك', 'لدن', 'لدي', 'لدى', 'لديه', 'لذلك', 'لعل', 'لعلَّ', 'لعمر', 'لقاء', 'لك', 'لكن', 'لكنَّ', 'لكنه', 'للامم', 'لم', 'لما', 'لمّا', 'لماذا', 'لن', 'لنا', 'له', 'لها', 'لهذا', 'لهم', 'لو', 'لوكالة', 'لولا', 'لوما', 'لي', 'ليت', 'ليرة', 'ليس', 'ليسب', 'م', 'ما', 'ما أفعله', 'ماانفك', 'ما انفك', 'مابرح', 'ما برح', 'مادام', 'ماذا', 'مارس', 'مازال', 'مافتئ', 'ماي', 'مائة', 'مايزال', 'مايو', 'متى', 'مثل', 'مذ', 'مرة', 'مرّة', 'مساء', 'مع', 'معاذ', 'معظم', 'معه', 'معها', 'مقابل', 'مكانَك', 'مكانكم', 'مكانكما', 'مكانكنّ', 'مليار', 'مليم', 'مليون', 'مما', 'من', 'منذ', 'منه', 'منها', 'مه', 'مهما', 'مئة', 'مئتان', 'ميم', 'ن', 'نَّ', 'نا', 'نبَّا', 'نحن', 'نحو', 'نَخْ', 'نعم', 'نفس', 'نفسك', 'نفسه', 'نفسها', 'نفسي', 'نهاية', 'نوفمبر', 'نون', 'نيسان', 'نيف', 'ه', 'ها', 'هاء', 'هَاتانِ', 'هَاتِه', 'هَاتِي', 'هَاتَيْنِ', 'هاكَ', 'هبّ', 'هَجْ', 'هذا', 'هَذا', 'هَذانِ', 'هذه', 'هَذِه', 'هَذِي', 'هَذَيْنِ', 'هكذا', 'هل', 'هلّا', 'هللة', 'هلم', 'هم', 'هما', 'همزة', 'هن', 'هنا', 'هناك', 'هنالك', 'هو', 'هؤلاء', 'هَؤلاء', 'هي', 'هى', 'هيا', 'هيّا', 'هيهات', 'هَيْهات', 'ؤ', 'و', 'و6', 'وا', 'وأبو', 'واحد', 'واضاف', 'واضافت', 'واكد', 'والتي', 'والذي', 'وأن', 'وإن', 'وان', 'واهاً', 'واو', 'واوضح', 'وبين', 'وثي', 'وجد', 'وجود', 'وراءَك', 'ورد', 'وُشْكَانَ', 'وعلى', 'وفي', 'وقال', 'وقالت', 'وقد', 'وقف', 'وكان', 'وكانت', 'وكل', 'ولا', 'ولايزال', 'ولكن', 'ولم', 'ولن', 'وله', 'وليس', 'وما', 'ومع', 'ومن', 'وهب', 'وهذا', 'وهو', 'وهي', 'وهى', 'وَيْ', 'ي', 'ى', 'ئ', 'ياء', 'يجري', 'يفعلان', 'يفعلون', 'يكون', 'يلي', 'يمكن', 'يمين', 'ين', 'يناير', 'ينبغي', 'يوان', 'يورو', 'يوليو', 'يوم', 'يونيو']
    def preprocess_text(text, language):
        # Check if the text is a string
        if not isinstance(text, str):
            return []
        # Determine which set of stopwords to use based on the language
        if language == 'English':
            stop_words = set(english_stopwords)
        elif language == 'Arabic':
            stop_words = set(arabic_stopwords)
        else:
            stop_words = set()
        
        # Tokenize and remove stopwords
        words = word_tokenize(text)
        filtered_words = [word for word in words if word.lower() not in stop_words]
        return filtered_words
        # Apply preprocessing
    df['processed_snippets'] = df.apply(lambda row: preprocess_text(row['snippet'], row['Language']), axis=1)

    # Create a new DataFrame for the results
    new_df = pd.DataFrame(columns=['review_id', 'word', 'data_id'])

    # Initialize an empty list to collect data
    data = []

    for index, row in df.iterrows():
        for word in row['processed_snippets']:
            data.append({
                'review_id': row['review_id'],
                'word': word,   
                'data_id': row['Identifier']})

    # Create a new DataFrame from the collected data
    new_df = pd.DataFrame(data, columns=['review_id', 'word', 'data_id'])

    new_df['lang'] = new_df['word'].apply(lambda x: 'En' if x.isascii() else 'Ar')

    new_df['word'] = new_df['word'].str.replace(r'[\d\W_]+', '', regex=True)

    #Drop rows where word is empty
    new_df = new_df[new_df['word'] != '']



    def normalize_property_names(record):
        normalized_record = {}
        for key, value in record.items():
            # Replace all non-alphanumeric characters with underscores
            # Ensure the key starts with an underscore if it begins with a digit
            normalized_key = re.sub(r'[^a-zA-Z0-9_]', '_', key)
            if normalized_key[0].isdigit():
                normalized_key = "_" + normalized_key
            normalized_record[normalized_key] = value
        return normalized_record

    def batch_insert_entities(table_client, entities):
        # Azure Table Storage limits batch operations to 100 transactions
        max_batch_size = 100
        for i in tqdm(range(0, len(entities), max_batch_size), desc="Inserting batches"):
            batch = entities[i:i+max_batch_size]
            try:
                response = table_client.submit_transaction(batch)
            except Exception as e:
                print(e)
                print("Retrying with individual transactions")
                print(batch)
                for operation, entity in batch:
                    try:
                        if operation == "upsert":
                            table_client.upsert_entity(entity)
                        elif operation == "delete":
                            table_client.delete_entity(entity["PartitionKey"], entity["RowKey"])
                    except Exception as e:
                        print(f"Failed to insert entity {entity}: {e}")
            # response=table_client.submit_transaction(batch)
            # print(response)
            
            

    def insert_data_to_azure_table_in_batches(connection_string, table_name, data):
        table_service = TableServiceClient.from_connection_string(conn_str=connection_string)
        table_client = table_service.get_table_client(table_name=table_name)
        entities_by_partition_key = defaultdict(list)

        for record in data:
            normalized_record = normalize_property_names(record)
            partition_key = str(normalized_record.get("data_id", "UnknownSource"))  # Assuming 'Name' can serve as PartitionKey
            word = normalized_record.get("word")  # Using 'Identifier' as RowKey
            lang = normalized_record.get("lang")  # Using 'Identifier' as RowKey
            review_id = normalized_record.get("review_id")  # Using 'Identifier' as RowKey
            row_key = str(word) + str(lang) + str(review_id)
            # print(row_key)
            row_key = str(row_key)
            entity = {"PartitionKey": partition_key, "RowKey": row_key}
            entity.update(normalized_record)
            entities_by_partition_key[partition_key].append(("upsert", entity))

        for partition_key, entities in tqdm(entities_by_partition_key.items(), desc="Processing partitions"):
            batch_insert_entities(table_client, entities)

    # Load your data
    data = new_df
    #drop duplicate words and keep the first
    data.drop_duplicates(subset=['review_id', 'word'], keep='first', inplace=True)

    # Convert DataFrame to dictionary records for insertion
    data_records = data.to_dict(orient="records")

    # Insert data to Azure Table
    connection_string = ''
    table_name = "BurgerWordCount"
    insert_data_to_azure_table_in_batches(connection_string, table_name, data_records)
        
