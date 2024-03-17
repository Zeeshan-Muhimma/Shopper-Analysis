import azure.functions as func
import logging
import pandas as pd
from pandas import Timestamp
import time
from io import BytesIO
import json
import io
from azure.data.tables import UpdateMode  # Ensure this import is at the top
from azure.storage.blob import BlobServiceClient, BlobClient
from datetime import datetime
from azure.data.tables import TableServiceClient
import hashlib
from tqdm import tqdm
import re
from collections import defaultdict
import nltk
from nltk.tokenize import word_tokenize
# from nltk.corpus import stopwords
import arabicstopwords.arabicstopwords as stp

english_stopwords = [",","0o", "0s", "3a", "3b", "3d", "6b", "6o", "a", "a1", "a2", "a3",\
    "a4", "ab", "able", "about", "above", "abst", "ac", "accordance", "according", \
    "accordingly", "across", "act", "actually", "ad", "added", "adj", "ae", "af", \
    "affected", "affecting", "affects", "after", "afterwards", "ag", "again", "against", \
    "ah", "ain", "ain't", "aj", "al", "all", "allow", "allows", "almost", "alone", "along", "already", "also", "although", "always", "am", "among", "amongst", "amoungst", "amount", "an", "and", "announce", "another", "any", "anybody", "anyhow", "anymore", "anyone", "anything", "anyway", "anyways", "anywhere", "ao", "ap", "apart", "apparently", "appear", "appreciate", "appropriate", "approximately", "ar", "are", "aren", "arent", "aren't", "arise", "around", "as", "a's", "aside", "ask", "asking", "associated", "at", "au", "auth", "av", "available", "aw", "away", "awfully", "ax", "ay", "az", "b", "b1", "b2", "b3", "ba", "back", "bc", "bd", "be", "became", "because", "become", "becomes", "becoming", "been", "before", "beforehand", "begin", "beginning", "beginnings", "begins", "behind", "being", "believe", "below", "beside", "besides", "best", "better", "between", "beyond", "bi", "bill", "biol", "bj", "bk", "bl", "bn", "both", "bottom", "bp", "br", "brief", "briefly", "bs", "bt", "bu", "but", "bx", "by", "c", "c1", "c2", "c3", "ca", "call", "came", "can", "cannot", "cant", "can't", "cause", "causes", "cc", "cd", "ce", "certain", "certainly", "cf", "cg", "ch", "changes", "ci", "cit", "cj", "cl", "clearly", "cm", "c'mon", "cn", "co", "com", "come", "comes", "con", "concerning", "consequently", "consider", "considering", "contain", "containing", "contains", "corresponding", "could", "couldn", "couldnt", "couldn't", "course", "cp", "cq", "cr", "cry", "cs", "c's", "ct", "cu", "currently", "cv", "cx", "cy", "cz", "d", "d2", "da", "date", "dc", "dd", "de", "definitely", "describe", "described", "despite", "detail", "df", "di", "did", "didn", "didn't", "different", "dj", "dk", "dl", "do", "does", "doesn", "doesn't", "doing", "don", "done", "don't", "down", "downwards", "dp", "dr", "ds", "dt", "du", "due", "during", "dx", "dy", "e", "e2", "e3", "ea", "each", "ec", "ed", "edu", "ee", "ef", "effect", "eg", "ei", "eight", "eighty", "either", "ej", "el", "eleven", "else", "elsewhere", "em", "empty", "en", "end", "ending", "enough", "entirely", "eo", "ep", "eq", "er", "es", "especially", "est", "et", "et-al", "etc", "eu", "ev", "even", "ever", "every", "everybody", "everyone", "everything", "everywhere", "ex", "exactly", "example", "except", "ey", "f", "f2", "fa", "far", "fc", "few", "ff", "fi", "fifteen", "fifth", "fify", "fill", "find", "fire", "first", "five", "fix", "fj", "fl", "fn", "fo", "followed", "following", "follows", "for", "former", "formerly", "forth", "forty", "found", "four", "fr", "from", "front", "fs", "ft", "fu", "full", "further", "furthermore", "fy", "g", "ga", "gave", "ge", "get", "gets", "getting", "gi", "give", "given", "gives", "giving", "gj", "gl", "go", "goes", "going", "gone", "got", "gotten", "gr", "greetings", "gs", "gy", "h", "h2", "h3", "had", "hadn", "hadn't", "happens", "hardly", "has", "hasn", "hasnt", "hasn't", "have", "haven", "haven't", "having", "he", "hed", "he'd", "he'll", "hello", "help", "hence", "her", "here", "hereafter", "hereby", "herein", "heres", "here's", "hereupon", "hers", "herself", "hes", "he's", "hh", "hi", "hid", "him", "himself", "his", "hither", "hj", "ho", "home", "hopefully", "how", "howbeit", "however", "how's", "hr", "hs", "http", "hu", "hundred", "hy", "i", "i2", "i3", "i4", "i6", "i7", "i8", "ia", "ib", "ibid", "ic", "id", "i'd", "ie", "if", "ig", "ignored", "ih", "ii", "ij", "il", "i'll", "im", "i'm", "immediate", "immediately", "importance", "important", "in", "inasmuch", "inc", "indeed", "index", "indicate", "indicated", "indicates", "information", "inner", "insofar", "instead", "interest", "into", "invention", "inward", "io", "ip", "iq", "ir", "is", "isn", "isn't", "it", "itd", "it'd", "it'll", "its", "it's", "itself", "iv", "i've", "ix", "iy", "iz", "j", "jj", "jr", "js", "jt", "ju", "just", "k", "ke", "keep", "keeps", "kept", "kg", "kj", "km", "know", "known", "knows", "ko", "l", "l2", "la", "largely", "last", "lately", "later", "latter", "latterly", "lb", "lc", "le", "least", "les", "less", "lest", "let", "lets", "let's", "lf", "like", "liked", "likely", "line", "little", "lj", "ll", "ll", "ln", "lo", "look", "looking", "looks", "los", "lr", "ls", "lt", "ltd", "m", "m2", "ma", "made", "mainly", "make", "makes", "many", "may", "maybe", "me", "mean", "means", "meantime", "meanwhile", "merely", "mg", "might", "mightn", "mightn't", "mill", "million", "mine", "miss", "ml", "mn", "mo", "more", "moreover", "most", "mostly", "move", "mr", "mrs", "ms", "mt", "mu", "much", "mug", "must", "mustn", "mustn't", "my", "myself", "n", "n2", "na", "name", "namely", "nay", "nc", "nd", "ne", "near", "nearly", "necessarily", "necessary", "need", "needn", "needn't", "needs", "neither", "never", "nevertheless", "new", "next", "ng", "ni", "nine", "ninety", "nj", "nl", "nn", "no", "nobody", "non", "none", "nonetheless", "noone", "nor", "normally", "nos", "not", "noted", "nothing", "novel", "now", "nowhere", "nr", "ns", "nt", "ny", "o", "oa", "ob", "obtain", "obtained", "obviously", "oc", "od", "of", "off", "often", "og", "oh", "oi", "oj", "ok", "okay", "ol", "old", "om", "omitted", "on", "once", "one", "ones", "only", "onto", "oo", "op", "oq", "or", "ord", "os", "ot", "other", "others", "otherwise", "ou", "ought", "our", "ours", "ourselves", "out", "outside", "over", "overall", "ow", "owing", "own", "ox", "oz", "p", "p1", "p2", "p3", "page", "pagecount", "pages", "par", "part", "particular", "particularly", "pas", "past", "pc", "pd", "pe", "per", "perhaps", "pf", "ph", "pi", "pj", "pk", "pl", "placed", "please", "plus", "pm", "pn", "po", "poorly", "possible", "possibly", "potentially", "pp", "pq", "pr", "predominantly", "present", "presumably", "previously", "primarily", "probably", "promptly", "proud", "provides", "ps", "pt", "pu", "put", "py", "q", "qj", "qu", "que", "quickly", "quite", "qv", "r", "r2", "ra", "ran", "rather", "rc", "rd", "re", "readily", "really", "reasonably", "recent", "recently", "ref", "refs", "regarding", "regardless", "regards", "related", "relatively", "research", "research-articl", "respectively", "resulted", "resulting", "results", "rf", "rh", "ri", "right", "rj", "rl", "rm", "rn", "ro", "rq", "rr", "rs", "rt", "ru", "run", "rv", "ry", "s", "s2", "sa", "said", "same", "saw", "say", "saying", "says", "sc", "sd", "se", "sec", "second", "secondly", "section", "see", "seeing", "seem", "seemed", "seeming", "seems", "seen", "self", "selves", "sensible", "sent", "serious", "seriously", "seven", "several", "sf", "shall", "shan", "shan't", "she", "shed", "she'd", "she'll", "shes", "she's", "should", "shouldn", "shouldn't", "should've", "show", "showed", "shown", "showns", "shows", "si", "side", "significant", "significantly", "similar", "similarly", "since", "sincere", "six", "sixty", "sj", "sl", "slightly", "sm", "sn", "so", "some", "somebody", "somehow", "someone", "somethan", "something", "sometime", "sometimes", "somewhat", "somewhere", "soon", "sorry", "sp", "specifically", "specified", "specify", "specifying", "sq", "sr", "ss", "st", "still", "stop", "strongly", "sub", "substantially", "successfully", "such", "sufficiently", "suggest", "sup", "sure", "sy", "system", "sz", "t", "t1", "t2", "t3", "take", "taken", "taking", "tb", "tc", "td", "te", "tell", "ten", "tends", "tf", "th", "than", "thank", "thanks", "thanx", "that", "that'll", "thats", "that's", "that've", "the", "their", "theirs", "them", "themselves", "then", "thence", "there", "thereafter", "thereby", "thered", "therefore", "therein", "there'll", "thereof", "therere", "theres", "there's", "thereto", "thereupon", "there've", "these", "they", "theyd", "they'd", "they'll", "theyre", "they're", "they've", "thickv", "thin", "think", "third", "this", "thorough", "thoroughly", "those", "thou", "though", "thoughh", "thousand", "three", "throug", "through", "throughout", "thru", "thus", "ti", "til", "tip", "tj", "tl", "tm", "tn", "to", "together", "too", "took", "top", "toward", "towards", "tp", "tq", "tr", "tried", "tries", "truly", "try", "trying", "ts", "t's", "tt", "tv", "twelve", "twenty", "twice", "two", "tx", "u", "u201d", "ue", "ui", "uj", "uk", "um", "un", "under", "unfortunately", "unless", "unlike", "unlikely", "until", "unto", "uo", "up", "upon", "ups", "ur", "us", "use", "used", "useful", "usefully", "usefulness", "uses", "using", "usually", "ut", "v", "va", "value", "various", "vd", "ve", "ve", "very", "via", "viz", "vj", "vo", "vol", "vols", "volumtype", "vq", "vs", "vt", "vu", "w", "wa", "want", "wants", "was", "wasn", "wasnt", "wasn't", "way", "we", "wed", "we'd", "welcome", "well", "we'll", "well-b", "went", "were", "we're", "weren", "werent", "weren't", "we've", "what", "whatever", "what'll", "whats", "what's", "when", "whence", "whenever", "when's", "where", "whereafter", "whereas", "whereby", "wherein", "wheres", "where's", "whereupon", "wherever", "whether", "which", "while", "whim", "whither", "who", "whod", "whoever", "whole", "who'll", "whom", "whomever", "whos", "who's", "whose", "why", "why's", "wi", "widely", "will", "willing", "wish", "with", "within", "without", "wo", "won", "wonder", "wont", "won't", "words", "world", "would", "wouldn", "wouldnt", "wouldn't", "www", "x", "x1", "x2", "x3", "xf", "xi", "xj", "xk", "xl", "xn", "xo", "xs", "xt", "xv", "xx", "y", "y2", "yes", "yet", "yj", "yl", "you", "youd", "you'd", "you'll", "your", "youre", "you're", "yours", "yourself", "yourselves", "you've", "yr", "ys", "yt", "z", "zero", "zi", "zz",]
# Assuming you have a list of Arabic stopwords
arabic_stopwords = ['،', 'ـ', 'ء', 'ءَ', 'آ', 'أ', 'ا', 'ا?', 'االا', 'االتى', 'آب', 'أبٌ', 'ابتدأ', 'أبدا', 'أبريل', 'أبو', 'ابين', 'اتخذ', 'اثر', 'اثنا', 'اثنان', 'اثني', 'اثنين', 'أجل', 'اجل', 'أجمع', 'أحد', 'احد', 'إحدى', 'أخٌ', 'أخبر', 'أخذ', 'آخر', 'اخرى', 'اخلولق', 'أخو', 'إذ', 'إذا', 'إذاً', 'اذا', 'آذار', 'إذما', 'إذن', 'أربع', 'أربعاء', 'أربعة', 'اربعة', 'أربعمائة', 'أربعمئة', 'اربعون', 'اربعين', 'ارتدّ', 'أرى', 'إزاء', 'استحال', 'أسكن', 'أصبح', 'اصبح', 'أصلا', 'آض', 'إضافي', 'أضحى', 'اضحى', 'اطار', 'أطعم', 'اعادة', 'أعطى', 'أعلم', 'اعلنت', 'أغسطس', 'أُفٍّ', 'أفٍّ', 'اف', 'أفريل', 'أفعل به', 'أقبل', 'أكتوبر', 'أكثر', 'اكثر', 'اكد', 'آل', 'أل', 'ألا', 'إلا', 'إلّا', 'الا', 'الاخيرة', 'الألاء', 'الألى', 'الآن', 'الان', 'الاول', 'الاولى', 'التي', 'التى', 'الثاني', 'الثانية', 'الحالي', 'الذاتي', 'الذي', 'الذى', 'الذين', 'السابق', 'ألف', 'الف', 'ألفى', 'اللاتي', 'اللتان', 'اللتيا', 'اللتين', 'اللذان', 'اللذين', 'اللواتي', 'الماضي', 'المقبل', 'الوقت', 'إلي', 'إلى', 'الي', 'الى', 'إلَيْكَ', 'إليكَ', 'إليكم', 'إليكما', 'إليكنّ', 'اليه', 'اليها', 'اليوم', 'أم', 'أما', 'أمّا', 'إما', 'إمّا', 'اما', 'أمام', 'امام', 'أمامك', 'أمامكَ', 'أمد', 'أمس', 'امس', 'أمسى', 'امسى', 'آمينَ', 'أن', 'أنًّ', 'إن', 'إنَّ', 'ان', 'أنا', 'آناء', 'أنبأ', 'انبرى', 'أنت', 'أنتِ', 'انت', 'أنتم', 'أنتما', 'أنتن', 'أنشأ', 'آنفا', 'أنفسكم', 'أنفسنا', 'أنفسهم', 'انقلب', 'أنه', 'إنه', 'انه', 'أنها', 'إنها', 'انها', 'أنّى', 'آه', 'آهٍ', 'آهِ', 'آهاً', 'أهلا', 'أو', 'او', 'أوت', 'أوشك', 'أول', 'اول', 'أولاء', 'أولالك', 'أولئك', 'أوّهْ', 'أي', 'أيّ', 'أى', 'إى', 'اي', 'اى', 'ا?ى', 'أيا', 'أيار', 'ايار', 'إياك', 'إياكم', 'إياكما', 'إياكن', 'ايام', 'ّأيّان', 'أيّان', 'إيانا', 'إياه', 'إياها', 'إياهم', 'إياهما', 'إياهن', 'إياي', 'أيضا', 'ايضا', 'أيلول', 'أين', 'إيهٍ', 'ب', 'باء', 'بات', 'باسم', 'بأن', 'بإن', 'بان', 'بخٍ', 'بد', 'بدلا', 'برس', 'بَسْ', 'بسّ', 'بسبب', 'بشكل', 'بضع', 'بطآن', 'بعد', 'بعدا', 'بعض', 'بعيدا', 'بغتة', 'بل', 'بَلْهَ', 'بلى', 'بن', 'به', 'بها', 'بهذا', 'بؤسا', 'بئس', 'بيد', 'بين', 'بينما', 'ة', 'ت', 'تاء', 'تارة', 'تاسع', 'تانِ', 'تانِك', 'تبدّل', 'تجاه', 'تحت', 'تحت\'', 'تحوّل', 'تخذ', 'ترك', 'تسع', 'تسعة', 'تسعمائة', 'تسعمئة', 'تسعون', 'تسعين', 'تشرين', 'تعسا', 'تعلَّم', 'تفعلان', 'تفعلون', 'تفعلين', 'تكون', 'تلقاء', 'تلك', 'تم', 'تموز', 'تِه', 'تِي', 'تَيْنِ', 'تينك', 'ث', 'ثاء', 'ثالث', 'ثامن', 'ثان', 'ثاني', 'ثانية', 'ثلاث', 'ثلاثاء', 'ثلاثة', 'ثلاثمائة', 'ثلاثمئة', 'ثلاثون', 'ثلاثين', 'ثم', 'ثمَّ', 'ثمّ', 'ثمان', 'ثمانمئة', 'ثمانون', 'ثماني', 'ثمانية', 'ثمانين', 'ثمّة', 'ثمنمئة', 'ج', 'جانفي', 'جدا', 'جعل', 'جلل', 'جمعة', 'جميع', 'جنيه', 'جوان', 'جويلية', 'جير', 'جيم', 'ح', 'حاء', 'حادي', 'حار', 'حاشا', 'حاليا', 'حاي', 'حبذا', 'حبيب', 'حتى', 'حجا', 'حدَث', 'حَذارِ', 'حرى', 'حزيران', 'حسب', 'حقا', 'حمٌ', 'حمدا', 'حمو', 'حوالى', 'حول', 'حيَّ', 'حيث', 'حيثما', 'حين', 'خ', 'خاء', 'خارج', 'خاصة', 'خال', 'خامس', 'خبَّر', 'خلا', 'خلافا', 'خلال', 'خلف', 'خمس', 'خمسة', 'خمسمائة', 'خمسمئة', 'خمسون', 'خمسين', 'خميس', 'د', 'دال', 'درهم', 'درى', 'دواليك', 'دولار', 'دون', 'دونك', 'ديسمبر', 'ديك', 'دينار', 'ذ', 'ذا', 'ذات', 'ذاك', 'ذال', 'ذانِ', 'ذانك', 'ذلك', 'ذِه', 'ذهب', 'ذو', 'ذِي', 'ذيت', 'ذَيْنِ', 'ذينك', 'ر', 'راء', 'رابع', 'راح', 'رأى', 'رُبَّ', 'رجع', 'رزق', 'رويدك', 'ريال', 'ريث', 'ز', 'زاي', 'زعم', 'زود', 'زيارة', 'س', 'ساء', 'سابع', 'سادس', 'سبت', 'سبتمبر', 'سبحان', 'سبع', 'سبعة', 'سبعمائة', 'سبعمئة', 'سبعون', 'سبعين', 'ست', 'ستة', 'ستكون', 'ستمائة', 'ستمئة', 'ستون', 'ستين', 'سحقا', 'سرا', 'سرعان', 'سقى', 'سمعا', 'سنة', 'سنتيم', 'سنوات', 'سوف', 'سوى', 'سين', 'ش', 'شباط', 'شبه', 'شَتَّانَ', 'شتانَ', 'شخصا', 'شرع', 'شمال', 'شيكل', 'شين', 'ص', 'صاد', 'صار', 'صباح', 'صباحا', 'صبر', 'صبرا', 'صدقا', 'صراحة', 'صفر', 'صهٍ', 'صهْ', 'ض', 'ضاد', 'ضحوة', 'ضد', 'ضمن', 'ط', 'طاء', 'طاق', 'طالما', 'طرا', 'طفق', 'طَق', 'ظ', 'ظاء', 'ظل', 'ظلّ', 'ظنَّ', 'ع', 'عاد', 'عاشر', 'عام', 'عاما', 'عامة', 'عجبا', 'عدَّ', 'عدا', 'عدة', 'عدد', 'عَدَسْ', 'عدم', 'عسى', 'عشر', 'عشرة', 'عشرون', 'عشرين', 'عل', 'علًّ', 'علق', 'علم', 'علي', 'على', 'عليك', 'عليه', 'عليها', 'عن', 'عند', 'عندما', 'عنه', 'عنها', 'عوض', 'عيانا', 'عين', 'غ', 'غادر', 'غالبا', 'غدا', 'غداة', 'غير', 'غين', 'ف', 'فاء', 'فأن', 'فإن', 'فان', 'فانه', 'فبراير', 'فرادى', 'فضلا', 'فعل', 'فقد', 'فقط', 'فكان', 'فلان', 'فلس', 'فما', 'فهو', 'فهي', 'فهى', 'فو', 'فوق', 'في', 'فى', 'فيفري', 'فيه', 'فيها', 'ق', 'قاطبة', 'قاف', 'قال', 'قام', 'قبل', 'قد', 'قرش', 'قطّ', 'قلما', 'قليل', 'قوة', 'ك', 'كاد', 'كاف', 'كأن', 'كأنّ', 'كان', 'كانت', 'كانون', 'كأيّ', 'كأيّن', 'كثيرا', 'كِخ', 'كذا', 'كذلك', 'كرب', 'كسا', 'كل', 'كلا', 'كلَّا', 'كلتا', 'كلم', 'كلّما', 'كم', 'كما', 'كن', 'كى', 'كيت', 'كيف', 'كيفما', 'ل', 'لا', 'لات', 'لازال', 'لاسيما', 'لا سيما', 'لام', 'لأن', 'لايزال', 'لبيك', 'لدن', 'لدي', 'لدى', 'لديه', 'لذلك', 'لعل', 'لعلَّ', 'لعمر', 'لقاء', 'لك', 'لكن', 'لكنَّ', 'لكنه', 'للامم', 'لم', 'لما', 'لمّا', 'لماذا', 'لن', 'لنا', 'له', 'لها', 'لهذا', 'لهم', 'لو', 'لوكالة', 'لولا', 'لوما', 'لي', 'ليت', 'ليرة', 'ليس', 'ليسب', 'م', 'ما', 'ما أفعله', 'ماانفك', 'ما انفك', 'مابرح', 'ما برح', 'مادام', 'ماذا', 'مارس', 'مازال', 'مافتئ', 'ماي', 'مائة', 'مايزال', 'مايو', 'متى', 'مثل', 'مذ', 'مرة', 'مرّة', 'مساء', 'مع', 'معاذ', 'معظم', 'معه', 'معها', 'مقابل', 'مكانَك', 'مكانكم', 'مكانكما', 'مكانكنّ', 'مليار', 'مليم', 'مليون', 'مما', 'من', 'منذ', 'منه', 'منها', 'مه', 'مهما', 'مئة', 'مئتان', 'ميم', 'ن', 'نَّ', 'نا', 'نبَّا', 'نحن', 'نحو', 'نَخْ', 'نعم', 'نفس', 'نفسك', 'نفسه', 'نفسها', 'نفسي', 'نهاية', 'نوفمبر', 'نون', 'نيسان', 'نيف', 'ه', 'ها', 'هاء', 'هَاتانِ', 'هَاتِه', 'هَاتِي', 'هَاتَيْنِ', 'هاكَ', 'هبّ', 'هَجْ', 'هذا', 'هَذا', 'هَذانِ', 'هذه', 'هَذِه', 'هَذِي', 'هَذَيْنِ', 'هكذا', 'هل', 'هلّا', 'هللة', 'هلم', 'هم', 'هما', 'همزة', 'هن', 'هنا', 'هناك', 'هنالك', 'هو', 'هؤلاء', 'هَؤلاء', 'هي', 'هى', 'هيا', 'هيّا', 'هيهات', 'هَيْهات', 'ؤ', 'و', 'و6', 'وا', 'وأبو', 'واحد', 'واضاف', 'واضافت', 'واكد', 'والتي', 'والذي', 'وأن', 'وإن', 'وان', 'واهاً', 'واو', 'واوضح', 'وبين', 'وثي', 'وجد', 'وجود', 'وراءَك', 'ورد', 'وُشْكَانَ', 'وعلى', 'وفي', 'وقال', 'وقالت', 'وقد', 'وقف', 'وكان', 'وكانت', 'وكل', 'ولا', 'ولايزال', 'ولكن', 'ولم', 'ولن', 'وله', 'وليس', 'وما', 'ومع', 'ومن', 'وهب', 'وهذا', 'وهو', 'وهي', 'وهى', 'وَيْ', 'ي', 'ى', 'ئ', 'ياء', 'يجري', 'يفعلان', 'يفعلون', 'يكون', 'يلي', 'يمكن', 'يمين', 'ين', 'يناير', 'ينبغي', 'يوان', 'يورو', 'يوليو', 'يوم', 'يونيو']

app = func.FunctionApp()

@app.blob_trigger(arg_name="myblob", path="gpt-reviews/{name}",
                               connection="muhimmablob_STORAGE") 
def blob_trigger(myblob: func.InputStream):
    logging.info(f"Python blob trigger function processed blob"
                f"Name: {myblob.name}"
                f"Blob Size: {myblob.length} bytes")
    blob_bytes = myblob.read()
    blob_stream = BytesIO(blob_bytes)
    blob_stream.seek(0) 
        # Read the blob content into a DataFrame
    df = pd.read_excel(blob_stream, index_col=None, header=0,engine='openpyxl') 
    df.rename(columns={
    'identifiers': 'data_id',
    'link': 'URL',
    'date': 'date',
    'Datee': 'Review_date',
    'category': 'category',  # This is technically not needed since the name is unchanged
    'keywords': 'keywords',  # This is technically not needed since the name is unchanged
    'likes': 'likes',  # This is technically not needed since the name is unchanged
    'user.link': 'name_details',
    'review_id': 'review_id',  # This is technically not needed since the name is unchanged
    'rating': 'star_rating',
    'user.name': 'user_name'
    }, inplace=True)
    df = df.groupby('review_id').agg('first').reset_index()
    df['review_ar'] = None
    df['review_eng'] = None
    df[['review_ar', 'review_eng']] = df.apply(categorize_reviews_updated, axis=1)
    df[['response_ar', 'response_eng']] = df.apply(categorize_reviews_updatedres, axis=1)
    columns_to_keep = [
    'data_id','URL', 'date', 'Review_date', 'category', 'keywords', 'likes',
    'name_details', 'response_ar', 'response_eng', 'review_id', 'star_rating', 'user_name',
    'review_ar', 'review_eng','snippet'
    ]
    df = df[columns_to_keep]
    # english_stopwords = set(stopwords.words('english'))
    # # Assuming you have a list of Arabic stopwords
    # arabic_stopwords = set(stopwords.words('arabic'))
    # Tokenize and remove stopwords for snippet, check arabic or english then apply stopwords
    df['words'] = df['snippet'].apply(lambda x: tokenize_and_remove_stopwords_simple(x, is_arabic(x)))

    df['words'] = df['review_ar'].apply(lambda x: word_tokenize(x) if pd.notnull(x) else [])
    df['words_arabic'] = df['review_ar'].apply(lambda x: tokenize_and_remove_stopwords(x, arabic_stopwords))

    # Tokenize and remove stopwords for review_eng
    df['words_english'] = df['review_eng'].apply(lambda x: tokenize_and_remove_stopwords(x, english_stopwords))
    df.drop(columns=['snippet'], inplace=True)
    #remove NaN values
    df = df.fillna('')
    #remove none
    df = df.replace('None','')
    #remove NaT values
    df = df.replace(pd.NaT,'')
    #remove nan values
    # df = df.replace(pd.NaT,'')
    
    # data_records = df.to_dict(orient="records")
    # Specify your Azure Storage account connection string and table names
    connection_string = ''
    table_name_arbi = 'burgerallreviews'
    # Load data from Excel file into DataFrame and normalize column names

    # Insert data into Azure Table in batches with tqdm progress bar
    batch_insert_data_to_azure_table(connection_string, table_name_arbi, df)
    logging.info(f"Data inserted into Azure Table Storage")

def is_arabic(text):
    if pd.isnull(text):
        return False
    # A more comprehensive check for Arabic script
    return any('\u0600' <= char <= '\u06FF' or '\u0750' <= char <= '\u077F' or '\u08A0' <= char <= '\u08FF' or '\uFB50' <= char <= '\uFDFF' or '\uFE70' <= char <= '\uFEFF' for char in text)

def categorize_reviews_updated(row):
    # Initialize the return values
    review_ar, review_eng = None, None
    
    # Check and categorize 'snippet'
    if pd.notnull(row['snippet']):
        if is_arabic(row['snippet']):
            review_ar = row['snippet']
        else:
            review_eng = row['snippet']
    
    # Check and categorize 'extracted_snippet.translated'
    if pd.notnull(row['extracted_snippet.translated']):
        if is_arabic(row['extracted_snippet.translated']):
            review_ar = row['extracted_snippet.translated']  # Prioritize or overwrite with Arabic content if exists
        else:
            review_eng = row['extracted_snippet.translated']  # Prioritize or overwrite with English content if exists
    
    return pd.Series([review_ar, review_eng])
def categorize_reviews_updatedres(row):
    # Initialize the return values
    response_ar, response_en = None, None
    
    # Check and categorize 'snippet'
    if pd.notnull(row['response.snippet']):
        if is_arabic(row['response.snippet']):
            response_ar = row['response.snippet']
        else:
            response_en = row['response.snippet']
    
    # Check and categorize 'extracted_snippet.translated'
    if pd.notnull(row['response.extracted_snippet.translated']):
        if is_arabic(row['response.extracted_snippet.translated']):
            response_ar = row['response.extracted_snippet.translated']  # Prioritize or overwrite with Arabic content if exists
        else:
            response_en = row['response.extracted_snippet.translated']  # Prioritize or overwrite with English content if exists
    
    return pd.Series([response_ar, response_en])


def tokenize_and_remove_stopwords(text, stopwords):
    if pd.isnull(text):
        return []
    # NLTK's word_tokenize is more appropriate for English
    words = word_tokenize(text)
    # Filter out the stopwords
    words_filtered = [word for word in words if word.lower() not in stopwords]
    return words_filtered

def tokenize_and_remove_stopwords_simple(text, is_arabic):
    if pd.isnull(text):
        return []
    # Tokenization using simple split
    words = text.split()
    # Determine the set of stopwords based on the language
    # english_stopwords = set(stopwords.words('english'))
    # # Assuming you have a list of Arabic stopwords
    # arabic_stopwords = set(stopwords.words('arabic'))
    stopwords = arabic_stopwords if is_arabic else english_stopwords
    # Filter out stopwords
    words_filtered = [word for word in words if word.lower() not in stopwords]
    return words_filtered


def normalize_property_names(record):
    normalized_record = {}
    for key, value in record.items():
        # Normalize key to ensure it adheres to Azure Table Storage naming rules
        normalized_key = key
        if normalized_key[0].isdigit():
            normalized_key = "n" + normalized_key  # Prepend 'n' if key starts with a digit
        normalized_key = normalized_key.replace(" ", "_").replace("-", "_")  # Replace spaces and hyphens with underscores
        # Add more replacements as per your data's requirements
        
        normalized_record[normalized_key] = value
    return normalized_record

def load_data_from_excel_to_dataframe(df):
    # Load DataFrame from Excel file
    # df = pd.read_excel(file_path)
    
    # Clean column names
    # df.columns = [column.strip().replace(" ", "_").replace("-", "_") for column in df.columns]
    # # print(df.columns)
    # df.drop('Unnamed:_0', axis=1, inplace=True)
    
    return df
def localize_timestamps(value):
    """
    If the value is a Timestamp, localize it to UTC.
    Otherwise, return the value unchanged.
    """
    if isinstance(value, Timestamp):
        # If the Timestamp is timezone naive, localize to UTC
        return value.tz_localize('UTC')
    return value


# def clean_value(value):
#     """Converts NaN and None to an empty string or other suitable placeholder."""
#     # Ensure we're working with scalar values
#     if pd.isna(value).any() or value is None:
#         return ""
#     elif isinstance(value, pd.Timestamp):
#         return value.isoformat()  # Ensure Timestamp is in ISO format
#     else:
#         return value
import json

def serialize_value(value):
    """
    Serialize lists to JSON strings. Leave other types unchanged.
    Adjust for other types as necessary.
    """
    if isinstance(value, list):
        return json.dumps(value,ensure_ascii=False)
    elif isinstance(value, Timestamp):
        # Localize timestamps to UTC if they're naive.
        return value.tz_localize('UTC') if value.tzinfo is None else value
    return value

def batch_insert_data_to_azure_table(connection_string, table_name, data, batch_size=100):
    table_service = TableServiceClient.from_connection_string(conn_str=connection_string)
    table_client = table_service.get_table_client(table_name=table_name)
    
    num_batches = (len(data) + batch_size - 1) // batch_size
    for batch_start in tqdm(range(0, len(data), batch_size), total=num_batches, desc="Inserting batches"):
        batch_end = min(batch_start + batch_size, len(data))
        batch_data = data.iloc[batch_start:batch_end]
        batch_entities = []
        
        for _, row in batch_data.iterrows():
            entity = {"PartitionKey": str(row.get("data_id", "UnknownSource")),
                      "RowKey": str(row.get('review_id'))}
            
            for column, value in row.items():
                # Serialize lists to JSON strings and localize timestamps
                cleaned_value = serialize_value(value)
                entity[column] = cleaned_value
            
            batch_entities.append(entity)
        
        for entity in batch_entities:
            try:
                table_client.upsert_entity(entity)
            except Exception as e:
                logging.error(f"Error inserting entity: {e}")

