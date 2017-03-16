/**
 * Copyright (c) 2013-2016, impossibl.com
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *  * Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 *  * Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *  * Neither the name of impossibl.com nor the names of its contributors may
 *    be used to endorse or promote products derived from this software
 *    without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */
package com.impossibl.postgres.utils;

import java.util.HashMap;
import java.util.Map;

public class Locales {

  private static final Map<String, String> LOCALES = new HashMap<>();

  static {
    // These are collected from: https://www.microsoft.com/resources/msdn/goglobal/default.mspx
    // with additional cross-comparing to *nix locale names. This is not exhaustive
    // by any stretch of the imagination.
    LOCALES.put("Afrikaans_South Africa", "af_ZA");
    LOCALES.put("Albanian_Albania", "sq_AL");
    LOCALES.put("Alsatian_France", "gsw_FR");
    LOCALES.put("Arabic_Algeria", "ar_DZ");
    LOCALES.put("Arabic_Bahrain", "ar_BH");
    LOCALES.put("Arabic_Egypt", "ar_EG");
    LOCALES.put("Arabic_Iraq", "ar_IQ");
    LOCALES.put("Arabic_Jordan", "ar_JO");
    LOCALES.put("Arabic_Kuwait", "ar_KW");
    LOCALES.put("Arabic_Lebanon", "ar_LB");
    LOCALES.put("Arabic_Libya", "ar_LY");
    LOCALES.put("Arabic_Morocco", "ar_MA");
    LOCALES.put("Arabic_Oman", "ar_OM");
    LOCALES.put("Arabic_Saudi Arabia", "ar_SA");
    LOCALES.put("Arabic_Syria", "ar_SY");
    LOCALES.put("Arabic_Tunisia", "ar_TN");
    LOCALES.put("Arabic_United Arab Emirates", "ar_AE");
    LOCALES.put("Arabic_Yemen", "ar_YE");
    LOCALES.put("Armenian_Armenia", "hy_AM");
    LOCALES.put("Assamese_India", "as_IN");
    LOCALES.put("Bashkir_Russia", "ba_RU");
    LOCALES.put("Basque_Spain", "eu_ES");
    LOCALES.put("Belarusian_Belarus", "be_BY");
    LOCALES.put("Bengali_Bangladesh", "bn_BD");
    LOCALES.put("Bengali_India", "bn_IN");
    LOCALES.put("Breton_France", "br_FR");
    LOCALES.put("Bulgarian_Bulgaria", "bg_BG");
    LOCALES.put("Catalan_Spain", "ca_ES");
    LOCALES.put("Croatian_Croatia", "hr_HR");
    LOCALES.put("Chinese_China", "zh_CN");
    LOCALES.put("Chinese_Singapore", "zh_SG");
    LOCALES.put("Chinese_Taiwan", "zh_TW");
    LOCALES.put("Corsican_France", "co_FR");
    LOCALES.put("Croatian_Croatia", "hr_HR");
    LOCALES.put("Croatian_Bosnia and Herzegovina", "hr_BA");
    LOCALES.put("Czech_Czech Republic", "cs_CZ");
    LOCALES.put("Danish_Denmark", "da_DK");
    LOCALES.put("Dari_Afghanistan", "prs_AF");
    LOCALES.put("Divehi_Maldives", "dv_MV");
    LOCALES.put("Dutch_Belgium", "nl_BE");
    LOCALES.put("Dutch_Netherlands", "nl_NL");
    LOCALES.put("English_Australia", "en_AU");
    LOCALES.put("English_Belize", "en_BZ");
    LOCALES.put("English_Canada", "en_CA");
    LOCALES.put("English_India", "en_IN");
    LOCALES.put("English_Ireland", "en_IE");
    LOCALES.put("English_Jamaica", "en_JM");
    LOCALES.put("English_Malaysia", "en_MY");
    LOCALES.put("English_New Zealand", "en_NZ");
    LOCALES.put("English_Republic of the Philippines", "en_PH");
    LOCALES.put("English_Singapore", "en_SG");
    LOCALES.put("English_South Africa", "en_ZA");
    LOCALES.put("English_Trinidad and Tobago", "en_TT");
    LOCALES.put("English_United Kingdom", "en_GB");
    LOCALES.put("English_United States", "en_US");
    LOCALES.put("English_Zimbabwe", "en_ZW");
    LOCALES.put("Estonian_Estonia", "et_EE");
    LOCALES.put("Faroese_Faroe Islands", "fo_FO");
    LOCALES.put("Filipino_Philippines", "fil_PH");
    LOCALES.put("Finnish_Finland", "fi_FI");
    LOCALES.put("French_Belgium", "fr_BE");
    LOCALES.put("French_Canada", "fr_CA");
    LOCALES.put("French_France", "fr_FR");
    LOCALES.put("French_Luxembourg", "fr_LU");
    LOCALES.put("French_Principality of Monaco", "fr_MC");
    LOCALES.put("French_Switzerland", "fr_CH");
    LOCALES.put("Frisian_Netherlands", "fy_NL");
    LOCALES.put("Galician_Spain", "gl_ES");
    LOCALES.put("Georgian_Georgia", "ka_GE");
    LOCALES.put("German_Austria", "de_AT");
    LOCALES.put("German_Germany", "de_DE");
    LOCALES.put("German_Liechtenstein", "de_LI");
    LOCALES.put("German_Luxembourg", "de_LU");
    LOCALES.put("German_Switzerland", "de_CH");
    LOCALES.put("Greek_Greece", "el_GR");
    LOCALES.put("Greenlandic_Greenland", "kl_GL");
    LOCALES.put("Gujarati_India", "gu_IN");
    LOCALES.put("Hebrew_Israel", "he_IL");
    LOCALES.put("Hindi_India", "hi_IN");
    LOCALES.put("Hungarian_Hungary", "hu_HU");
    LOCALES.put("Icelandic_Iceland", "is_IS");
    LOCALES.put("Igbo_Nigeria", "ig_NG");
    LOCALES.put("Indonesian_Indonesia", "id_ID");
    LOCALES.put("Irish_Ireland", "ga_IE");
    LOCALES.put("Italian_Italy", "it_IT");
    LOCALES.put("Italian_Switzerland", "it_CH");
    LOCALES.put("Japanese_Japan", "ja_JP");
    LOCALES.put("Kannada_India", "kn_IN");
    LOCALES.put("Kazakh_Kazakhstan", "kk_KZ");
    LOCALES.put("Khmer_Cambodia", "km_KH");
    LOCALES.put("Kinyarwanda_Rwanda", "rw_RW");
    LOCALES.put("Kiswahili_Kenya", "sw_KE");
    LOCALES.put("Konkani_India", "kok_IN");
    LOCALES.put("Korean_Korea", "ko_KR");
    LOCALES.put("Kyrgyz_Kyrgyzstan", "ky_KG");
    LOCALES.put("Latvian_Latvia", "lv_LV");
    LOCALES.put("Lithuanian_Lithuania", "lt_LT");
    LOCALES.put("Lower Sorbian_Germany", "wee_DE");
    LOCALES.put("Luxembourgish_Luxembourg", "lb_LU");
    LOCALES.put("Macedonian_Former Yugoslav Republic of Macedonia", "mk_MK");
    LOCALES.put("Malay_Brunei Darussalam", "ms_BN");
    LOCALES.put("Malay_Malaysia", "ms_MY");
    LOCALES.put("Malayalam_India", "ml_IN");
    LOCALES.put("Maltese_Malta", "mt_MT");
    LOCALES.put("Maori_New Zealand", "mi_NZ");
    LOCALES.put("Mapudungun_Chile", "arn_CL");
    LOCALES.put("Marathi_India", "mr_IN");
    LOCALES.put("Mohawk_Canada", "moh_CA");
    LOCALES.put("Mongolian_Mongolia", "mn_MN");
    LOCALES.put("Nepali_Nepal", "ne_NP");
    LOCALES.put("Norwegian_Norway", "nn_NO");
    LOCALES.put("Occitan_France", "oc_FR");
    LOCALES.put("Oriya_India", "or_IN");
    LOCALES.put("Pashto_Afghanistan", "ps_AF");
    LOCALES.put("Persian_Iran", "fa_IR");
    LOCALES.put("Polish_Poland", "pl_PL");
    LOCALES.put("Portuguese_Brazil", "pt_BR");
    LOCALES.put("Portuguese_Portugal", "pt_PT");
    LOCALES.put("Punjabi_India", "pa_IN");
    LOCALES.put("Quechua_Bolivia", "quz_BO");
    LOCALES.put("Quechua_Ecuador", "quz_EC");
    LOCALES.put("Quechua_Peru", "quz_PE");
    LOCALES.put("Romanian_Romania", "ro_RO");
    LOCALES.put("Romansh_Switzerland", "rm_CH");
    LOCALES.put("Russian_Russia", "ru_RU");
    LOCALES.put("Sanskrit_India", "sa_IN");
    LOCALES.put("Setswana_South Africa", "tn_ZA");
    LOCALES.put("Sinhala_Sri Lanka", "si_LK");
    LOCALES.put("Slovak_Slovakia", "sk_SK");
    LOCALES.put("Slovenian_Slovenia", "sl_SL");
    LOCALES.put("Spanish_Argentina", "es_AR");
    LOCALES.put("Spanish_Bolivia", "es_BO");
    LOCALES.put("Spanish_Chile", "es_CL");
    LOCALES.put("Spanish_Colombia", "es_CO");
    LOCALES.put("Spanish_Dominican Republic", "es_DO");
    LOCALES.put("Spanish_Ecuador", "es_EC");
    LOCALES.put("Spanish_El Salvador", "es_SV");
    LOCALES.put("Spanish_Guatemala", "es_GT");
    LOCALES.put("Spanish_Honduras", "es_HN");
    LOCALES.put("Spanish_Mexico", "es_MX");
    LOCALES.put("Spanish_Nicaragua", "es_NI");
    LOCALES.put("Spanish_Panama", "es_PA");
    LOCALES.put("Spanish_Paraguay", "es_PY");
    LOCALES.put("Spanish_Peru", "es_PE");
    LOCALES.put("Spanish_Puerto Rico", "es_PR");
    LOCALES.put("Spanish_Spain", "es_ES");
    LOCALES.put("Spanish_United States", "es_US");
    LOCALES.put("Spanish_Uruguay", "es_UY");
    LOCALES.put("Spanish_Venezuela", "es_VE");
    LOCALES.put("Swedish_Finland", "sv_FI");
    LOCALES.put("Swedish_Sweden", "sv_SE");
    LOCALES.put("Syriac_Syria", "syr_SY");
    LOCALES.put("Tamil_India", "ta_IN");
    LOCALES.put("Tatar_Russia", "tt_RU");
    LOCALES.put("Telugu_India", "te_IN");
    LOCALES.put("Thai_Thailand", "th_TH");
    LOCALES.put("Tibetan_China", "bo_CN");
    LOCALES.put("Turkish_Turkey", "tr_TR");
    LOCALES.put("Turkmen_Turkmenistan", "tk_TM");
    LOCALES.put("Uighur_China", "ug_CN");
    LOCALES.put("Ukrainian_Ukraine", "uk_UA");
    LOCALES.put("Upper Sorbian_Germany", "wen_DE");
    LOCALES.put("Urdu_Islamic Republic of Pakistan", "ur_PK");
    LOCALES.put("Vietnamese_Vietnam", "vi_VN");
    LOCALES.put("Welsh_United Kingdom", "cy_GB");
    LOCALES.put("Wolof_Senegal", "wo_SN");
    LOCALES.put("Yakut_Russia", "sah_RU");
    LOCALES.put("Yi_China", "ii_CN");
    LOCALES.put("Yoruba_Nigeria", "yo_NG");
    LOCALES.put("Norwegian Nynorsk_Norway", "nn_NO");
  }

  private Locales() {

  }

  public static String getJavaCompatibleLocale(String windowsLocale) {
    if (windowsLocale.startsWith("Norwegian Bokm")) {
      return "nb_NO";
    }
    return LOCALES.get(windowsLocale);
  }

}
