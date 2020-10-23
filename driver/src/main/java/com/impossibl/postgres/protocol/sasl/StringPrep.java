/*
 * Copyright 2019, OnGres.
 *
 * Redistribution and use in source and binary forms, with or without modification, are permitted provided that the
 * following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice, this list of conditions and the following
 * disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the
 * following disclaimer in the documentation and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES,
 * INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
 * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 */

package com.impossibl.postgres.protocol.sasl;

import java.util.List;

public class StringPrep {

  public static boolean unassignedCodePoints(int codepoint) {
    return
        codepoint == 545 ||
        codepoint >= 564 && codepoint <= 591 ||
        codepoint >= 686 && codepoint <= 687 ||
        codepoint >= 751 && codepoint <= 767 ||
        codepoint >= 848 && codepoint <= 863 ||
        codepoint >= 880 && codepoint <= 883 ||
        codepoint >= 886 && codepoint <= 889 ||
        codepoint >= 891 && codepoint <= 893 ||
        codepoint >= 895 && codepoint <= 899 ||
        codepoint == 907 ||
        codepoint == 909 ||
        codepoint == 930 ||
        codepoint == 975 ||
        codepoint >= 1015 && codepoint <= 1023 ||
        codepoint == 1159 ||
        codepoint == 1231 ||
        codepoint >= 1270 && codepoint <= 1271 ||
        codepoint >= 1274 && codepoint <= 1279 ||
        codepoint >= 1296 && codepoint <= 1328 ||
        codepoint >= 1367 && codepoint <= 1368 ||
        codepoint == 1376 ||
        codepoint == 1416 ||
        codepoint >= 1419 && codepoint <= 1424 ||
        codepoint == 1442 ||
        codepoint == 1466 ||
        codepoint >= 1477 && codepoint <= 1487 ||
        codepoint >= 1515 && codepoint <= 1519 ||
        codepoint >= 1525 && codepoint <= 1547 ||
        codepoint >= 1549 && codepoint <= 1562 ||
        codepoint >= 1564 && codepoint <= 1566 ||
        codepoint == 1568 ||
        codepoint >= 1595 && codepoint <= 1599 ||
        codepoint >= 1622 && codepoint <= 1631 ||
        codepoint >= 1774 && codepoint <= 1775 ||
        codepoint == 1791 ||
        codepoint == 1806 ||
        codepoint >= 1837 && codepoint <= 1839 ||
        codepoint >= 1867 && codepoint <= 1919 ||
        codepoint >= 1970 && codepoint <= 2304 ||
        codepoint == 2308 ||
        codepoint >= 2362 && codepoint <= 2363 ||
        codepoint >= 2382 && codepoint <= 2383 ||
        codepoint >= 2389 && codepoint <= 2391 ||
        codepoint >= 2417 && codepoint <= 2432 ||
        codepoint == 2436 ||
        codepoint >= 2445 && codepoint <= 2446 ||
        codepoint >= 2449 && codepoint <= 2450 ||
        codepoint == 2473 ||
        codepoint == 2481 ||
        codepoint >= 2483 && codepoint <= 2485 ||
        codepoint >= 2490 && codepoint <= 2491 ||
        codepoint == 2493 ||
        codepoint >= 2501 && codepoint <= 2502 ||
        codepoint >= 2505 && codepoint <= 2506 ||
        codepoint >= 2510 && codepoint <= 2518 ||
        codepoint >= 2520 && codepoint <= 2523 ||
        codepoint == 2526 ||
        codepoint >= 2532 && codepoint <= 2533 ||
        codepoint >= 2555 && codepoint <= 2561 ||
        codepoint >= 2563 && codepoint <= 2564 ||
        codepoint >= 2571 && codepoint <= 2574 ||
        codepoint >= 2577 && codepoint <= 2578 ||
        codepoint == 2601 ||
        codepoint == 2609 ||
        codepoint == 2612 ||
        codepoint == 2615 ||
        codepoint >= 2618 && codepoint <= 2619 ||
        codepoint == 2621 ||
        codepoint >= 2627 && codepoint <= 2630 ||
        codepoint >= 2633 && codepoint <= 2634 ||
        codepoint >= 2638 && codepoint <= 2648 ||
        codepoint == 2653 ||
        codepoint >= 2655 && codepoint <= 2661 ||
        codepoint >= 2677 && codepoint <= 2688 ||
        codepoint == 2692 ||
        codepoint == 2700 ||
        codepoint == 2702 ||
        codepoint == 2706 ||
        codepoint == 2729 ||
        codepoint == 2737 ||
        codepoint == 2740 ||
        codepoint >= 2746 && codepoint <= 2747 ||
        codepoint == 2758 ||
        codepoint == 2762 ||
        codepoint >= 2766 && codepoint <= 2767 ||
        codepoint >= 2769 && codepoint <= 2783 ||
        codepoint >= 2785 && codepoint <= 2789 ||
        codepoint >= 2800 && codepoint <= 2816 ||
        codepoint == 2820 ||
        codepoint >= 2829 && codepoint <= 2830 ||
        codepoint >= 2833 && codepoint <= 2834 ||
        codepoint == 2857 ||
        codepoint == 2865 ||
        codepoint >= 2868 && codepoint <= 2869 ||
        codepoint >= 2874 && codepoint <= 2875 ||
        codepoint >= 2884 && codepoint <= 2886 ||
        codepoint >= 2889 && codepoint <= 2890 ||
        codepoint >= 2894 && codepoint <= 2901 ||
        codepoint >= 2904 && codepoint <= 2907 ||
        codepoint == 2910 ||
        codepoint >= 2914 && codepoint <= 2917 ||
        codepoint >= 2929 && codepoint <= 2945 ||
        codepoint == 2948 ||
        codepoint >= 2955 && codepoint <= 2957 ||
        codepoint == 2961 ||
        codepoint >= 2966 && codepoint <= 2968 ||
        codepoint == 2971 ||
        codepoint == 2973 ||
        codepoint >= 2976 && codepoint <= 2978 ||
        codepoint >= 2981 && codepoint <= 2983 ||
        codepoint >= 2987 && codepoint <= 2989 ||
        codepoint == 2998 ||
        codepoint >= 3002 && codepoint <= 3005 ||
        codepoint >= 3011 && codepoint <= 3013 ||
        codepoint == 3017 ||
        codepoint >= 3022 && codepoint <= 3030 ||
        codepoint >= 3032 && codepoint <= 3046 ||
        codepoint >= 3059 && codepoint <= 3072 ||
        codepoint == 3076 ||
        codepoint == 3085 ||
        codepoint == 3089 ||
        codepoint == 3113 ||
        codepoint == 3124 ||
        codepoint >= 3130 && codepoint <= 3133 ||
        codepoint == 3141 ||
        codepoint == 3145 ||
        codepoint >= 3150 && codepoint <= 3156 ||
        codepoint >= 3159 && codepoint <= 3167 ||
        codepoint >= 3170 && codepoint <= 3173 ||
        codepoint >= 3184 && codepoint <= 3201 ||
        codepoint == 3204 ||
        codepoint == 3213 ||
        codepoint == 3217 ||
        codepoint == 3241 ||
        codepoint == 3252 ||
        codepoint >= 3258 && codepoint <= 3261 ||
        codepoint == 3269 ||
        codepoint == 3273 ||
        codepoint >= 3278 && codepoint <= 3284 ||
        codepoint >= 3287 && codepoint <= 3293 ||
        codepoint == 3295 ||
        codepoint >= 3298 && codepoint <= 3301 ||
        codepoint >= 3312 && codepoint <= 3329 ||
        codepoint == 3332 ||
        codepoint == 3341 ||
        codepoint == 3345 ||
        codepoint == 3369 ||
        codepoint >= 3386 && codepoint <= 3389 ||
        codepoint >= 3396 && codepoint <= 3397 ||
        codepoint == 3401 ||
        codepoint >= 3406 && codepoint <= 3414 ||
        codepoint >= 3416 && codepoint <= 3423 ||
        codepoint >= 3426 && codepoint <= 3429 ||
        codepoint >= 3440 && codepoint <= 3457 ||
        codepoint == 3460 ||
        codepoint >= 3479 && codepoint <= 3481 ||
        codepoint == 3506 ||
        codepoint == 3516 ||
        codepoint >= 3518 && codepoint <= 3519 ||
        codepoint >= 3527 && codepoint <= 3529 ||
        codepoint >= 3531 && codepoint <= 3534 ||
        codepoint == 3541 ||
        codepoint == 3543 ||
        codepoint >= 3552 && codepoint <= 3569 ||
        codepoint >= 3573 && codepoint <= 3584 ||
        codepoint >= 3643 && codepoint <= 3646 ||
        codepoint >= 3676 && codepoint <= 3712 ||
        codepoint == 3715 ||
        codepoint >= 3717 && codepoint <= 3718 ||
        codepoint == 3721 ||
        codepoint >= 3723 && codepoint <= 3724 ||
        codepoint >= 3726 && codepoint <= 3731 ||
        codepoint == 3736 ||
        codepoint == 3744 ||
        codepoint == 3748 ||
        codepoint == 3750 ||
        codepoint >= 3752 && codepoint <= 3753 ||
        codepoint == 3756 ||
        codepoint == 3770 ||
        codepoint >= 3774 && codepoint <= 3775 ||
        codepoint == 3781 ||
        codepoint == 3783 ||
        codepoint >= 3790 && codepoint <= 3791 ||
        codepoint >= 3802 && codepoint <= 3803 ||
        codepoint >= 3806 && codepoint <= 3839 ||
        codepoint == 3912 ||
        codepoint >= 3947 && codepoint <= 3952 ||
        codepoint >= 3980 && codepoint <= 3983 ||
        codepoint == 3992 ||
        codepoint == 4029 ||
        codepoint >= 4045 && codepoint <= 4046 ||
        codepoint >= 4048 && codepoint <= 4095 ||
        codepoint == 4130 ||
        codepoint == 4136 ||
        codepoint == 4139 ||
        codepoint >= 4147 && codepoint <= 4149 ||
        codepoint >= 4154 && codepoint <= 4159 ||
        codepoint >= 4186 && codepoint <= 4255 ||
        codepoint >= 4294 && codepoint <= 4303 ||
        codepoint >= 4345 && codepoint <= 4346 ||
        codepoint >= 4348 && codepoint <= 4351 ||
        codepoint >= 4442 && codepoint <= 4446 ||
        codepoint >= 4515 && codepoint <= 4519 ||
        codepoint >= 4602 && codepoint <= 4607 ||
        codepoint == 4615 ||
        codepoint == 4679 ||
        codepoint == 4681 ||
        codepoint >= 4686 && codepoint <= 4687 ||
        codepoint == 4695 ||
        codepoint == 4697 ||
        codepoint >= 4702 && codepoint <= 4703 ||
        codepoint == 4743 ||
        codepoint == 4745 ||
        codepoint >= 4750 && codepoint <= 4751 ||
        codepoint == 4783 ||
        codepoint == 4785 ||
        codepoint >= 4790 && codepoint <= 4791 ||
        codepoint == 4799 ||
        codepoint == 4801 ||
        codepoint >= 4806 && codepoint <= 4807 ||
        codepoint == 4815 ||
        codepoint == 4823 ||
        codepoint == 4847 ||
        codepoint == 4879 ||
        codepoint == 4881 ||
        codepoint >= 4886 && codepoint <= 4887 ||
        codepoint == 4895 ||
        codepoint == 4935 ||
        codepoint >= 4955 && codepoint <= 4960 ||
        codepoint >= 4989 && codepoint <= 5023 ||
        codepoint >= 5109 && codepoint <= 5120 ||
        codepoint >= 5751 && codepoint <= 5759 ||
        codepoint >= 5789 && codepoint <= 5791 ||
        codepoint >= 5873 && codepoint <= 5887 ||
        codepoint == 5901 ||
        codepoint >= 5909 && codepoint <= 5919 ||
        codepoint >= 5943 && codepoint <= 5951 ||
        codepoint >= 5972 && codepoint <= 5983 ||
        codepoint == 5997 ||
        codepoint == 6001 ||
        codepoint >= 6004 && codepoint <= 6015 ||
        codepoint >= 6109 && codepoint <= 6111 ||
        codepoint >= 6122 && codepoint <= 6143 ||
        codepoint == 6159 ||
        codepoint >= 6170 && codepoint <= 6175 ||
        codepoint >= 6264 && codepoint <= 6271 ||
        codepoint >= 6314 && codepoint <= 7679 ||
        codepoint >= 7836 && codepoint <= 7839 ||
        codepoint >= 7930 && codepoint <= 7935 ||
        codepoint >= 7958 && codepoint <= 7959 ||
        codepoint >= 7966 && codepoint <= 7967 ||
        codepoint >= 8006 && codepoint <= 8007 ||
        codepoint >= 8014 && codepoint <= 8015 ||
        codepoint == 8024 ||
        codepoint == 8026 ||
        codepoint == 8028 ||
        codepoint == 8030 ||
        codepoint >= 8062 && codepoint <= 8063 ||
        codepoint == 8117 ||
        codepoint == 8133 ||
        codepoint >= 8148 && codepoint <= 8149 ||
        codepoint == 8156 ||
        codepoint >= 8176 && codepoint <= 8177 ||
        codepoint == 8181 ||
        codepoint == 8191 ||
        codepoint >= 8275 && codepoint <= 8278 ||
        codepoint >= 8280 && codepoint <= 8286 ||
        codepoint >= 8292 && codepoint <= 8297 ||
        codepoint >= 8306 && codepoint <= 8307 ||
        codepoint >= 8335 && codepoint <= 8351 ||
        codepoint >= 8370 && codepoint <= 8399 ||
        codepoint >= 8427 && codepoint <= 8447 ||
        codepoint >= 8507 && codepoint <= 8508 ||
        codepoint >= 8524 && codepoint <= 8530 ||
        codepoint >= 8580 && codepoint <= 8591 ||
        codepoint >= 9167 && codepoint <= 9215 ||
        codepoint >= 9255 && codepoint <= 9279 ||
        codepoint >= 9291 && codepoint <= 9311 ||
        codepoint == 9471 ||
        codepoint >= 9748 && codepoint <= 9749 ||
        codepoint == 9752 ||
        codepoint >= 9854 && codepoint <= 9855 ||
        codepoint >= 9866 && codepoint <= 9984 ||
        codepoint == 9989 ||
        codepoint >= 9994 && codepoint <= 9995 ||
        codepoint == 10024 ||
        codepoint == 10060 ||
        codepoint == 10062 ||
        codepoint >= 10067 && codepoint <= 10069 ||
        codepoint == 10071 ||
        codepoint >= 10079 && codepoint <= 10080 ||
        codepoint >= 10133 && codepoint <= 10135 ||
        codepoint == 10160 ||
        codepoint >= 10175 && codepoint <= 10191 ||
        codepoint >= 10220 && codepoint <= 10223 ||
        codepoint >= 11008 && codepoint <= 11903 ||
        codepoint == 11930 ||
        codepoint >= 12020 && codepoint <= 12031 ||
        codepoint >= 12246 && codepoint <= 12271 ||
        codepoint >= 12284 && codepoint <= 12287 ||
        codepoint == 12352 ||
        codepoint >= 12439 && codepoint <= 12440 ||
        codepoint >= 12544 && codepoint <= 12548 ||
        codepoint >= 12589 && codepoint <= 12592 ||
        codepoint == 12687 ||
        codepoint >= 12728 && codepoint <= 12783 ||
        codepoint >= 12829 && codepoint <= 12831 ||
        codepoint >= 12868 && codepoint <= 12880 ||
        codepoint >= 12924 && codepoint <= 12926 ||
        codepoint >= 13004 && codepoint <= 13007 ||
        codepoint == 13055 ||
        codepoint >= 13175 && codepoint <= 13178 ||
        codepoint >= 13278 && codepoint <= 13279 ||
        codepoint == 13311 ||
        codepoint >= 19894 && codepoint <= 19967 ||
        codepoint >= 40870 && codepoint <= 40959 ||
        codepoint >= 42125 && codepoint <= 42127 ||
        codepoint >= 42183 && codepoint <= 44031 ||
        codepoint >= 55204 && codepoint <= 55295 ||
        codepoint >= 64046 && codepoint <= 64047 ||
        codepoint >= 64107 && codepoint <= 64255 ||
        codepoint >= 64263 && codepoint <= 64274 ||
        codepoint >= 64280 && codepoint <= 64284 ||
        codepoint == 64311 ||
        codepoint == 64317 ||
        codepoint == 64319 ||
        codepoint == 64322 ||
        codepoint == 64325 ||
        codepoint >= 64434 && codepoint <= 64466 ||
        codepoint >= 64832 && codepoint <= 64847 ||
        codepoint >= 64912 && codepoint <= 64913 ||
        codepoint >= 64968 && codepoint <= 64975 ||
        codepoint >= 65021 && codepoint <= 65023 ||
        codepoint >= 65040 && codepoint <= 65055 ||
        codepoint >= 65060 && codepoint <= 65071 ||
        codepoint >= 65095 && codepoint <= 65096 ||
        codepoint == 65107 ||
        codepoint == 65127 ||
        codepoint >= 65132 && codepoint <= 65135 ||
        codepoint == 65141 ||
        codepoint >= 65277 && codepoint <= 65278 ||
        codepoint == 65280 ||
        codepoint >= 65471 && codepoint <= 65473 ||
        codepoint >= 65480 && codepoint <= 65481 ||
        codepoint >= 65488 && codepoint <= 65489 ||
        codepoint >= 65496 && codepoint <= 65497 ||
        codepoint >= 65501 && codepoint <= 65503 ||
        codepoint == 65511 ||
        codepoint >= 65519 && codepoint <= 65528 ||
        codepoint >= 65536 && codepoint <= 66303 ||
        codepoint == 66335 ||
        codepoint >= 66340 && codepoint <= 66351 ||
        codepoint >= 66379 && codepoint <= 66559 ||
        codepoint >= 66598 && codepoint <= 66599 ||
        codepoint >= 66638 && codepoint <= 118783 ||
        codepoint >= 119030 && codepoint <= 119039 ||
        codepoint >= 119079 && codepoint <= 119081 ||
        codepoint >= 119262 && codepoint <= 119807 ||
        codepoint == 119893 ||
        codepoint == 119965 ||
        codepoint >= 119968 && codepoint <= 119969 ||
        codepoint >= 119971 && codepoint <= 119972 ||
        codepoint >= 119975 && codepoint <= 119976 ||
        codepoint == 119981 ||
        codepoint == 119994 ||
        codepoint == 119996 ||
        codepoint == 120001 ||
        codepoint == 120004 ||
        codepoint == 120070 ||
        codepoint >= 120075 && codepoint <= 120076 ||
        codepoint == 120085 ||
        codepoint == 120093 ||
        codepoint == 120122 ||
        codepoint == 120127 ||
        codepoint == 120133 ||
        codepoint >= 120135 && codepoint <= 120137 ||
        codepoint == 120145 ||
        codepoint >= 120484 && codepoint <= 120487 ||
        codepoint >= 120778 && codepoint <= 120781 ||
        codepoint >= 120832 && codepoint <= 131069 ||
        codepoint >= 173783 && codepoint <= 194559 ||
        codepoint >= 195102 && codepoint <= 196605 ||
        codepoint >= 196608 && codepoint <= 262141 ||
        codepoint >= 262144 && codepoint <= 327677 ||
        codepoint >= 327680 && codepoint <= 393213 ||
        codepoint >= 393216 && codepoint <= 458749 ||
        codepoint >= 458752 && codepoint <= 524285 ||
        codepoint >= 524288 && codepoint <= 589821 ||
        codepoint >= 589824 && codepoint <= 655357 ||
        codepoint >= 655360 && codepoint <= 720893 ||
        codepoint >= 720896 && codepoint <= 786429 ||
        codepoint >= 786432 && codepoint <= 851965 ||
        codepoint >= 851968 && codepoint <= 917501 ||
        codepoint == 917504 ||
        codepoint >= 917506 && codepoint <= 917535 ||
        codepoint >= 917632 && codepoint <= 983037
        ;
  }

  public static boolean mapToNothing(int codepoint) {
    return
        codepoint == 173 ||
        codepoint == 847 ||
        codepoint == 6150 ||
        codepoint == 6155 ||
        codepoint == 6156 ||
        codepoint == 6157 ||
        codepoint == 8203 ||
        codepoint == 8204 ||
        codepoint == 8205 ||
        codepoint == 8288 ||
        codepoint == 65024 ||
        codepoint == 65025 ||
        codepoint == 65026 ||
        codepoint == 65027 ||
        codepoint == 65028 ||
        codepoint == 65029 ||
        codepoint == 65030 ||
        codepoint == 65031 ||
        codepoint == 65032 ||
        codepoint == 65033 ||
        codepoint == 65034 ||
        codepoint == 65035 ||
        codepoint == 65036 ||
        codepoint == 65037 ||
        codepoint == 65038 ||
        codepoint == 65039 ||
        codepoint == 65279
        ;
  }

  public static int[] mapUsedWithNfkc(int codepoint) {
    switch (codepoint) {
      case (65):
        return new int[] {97};
      case (66):
        return new int[] {98};
      case (67):
        return new int[] {99};
      case (68):
        return new int[] {100};
      case (69):
        return new int[] {101};
      case (70):
        return new int[] {102};
      case (71):
        return new int[] {103};
      case (72):
        return new int[] {104};
      case (73):
        return new int[] {105};
      case (74):
        return new int[] {106};
      case (75):
        return new int[] {107};
      case (76):
        return new int[] {108};
      case (77):
        return new int[] {109};
      case (78):
        return new int[] {110};
      case (79):
        return new int[] {111};
      case (80):
        return new int[] {112};
      case (81):
        return new int[] {113};
      case (82):
        return new int[] {114};
      case (83):
        return new int[] {115};
      case (84):
        return new int[] {116};
      case (85):
        return new int[] {117};
      case (86):
        return new int[] {118};
      case (87):
        return new int[] {119};
      case (88):
        return new int[] {120};
      case (89):
        return new int[] {121};
      case (90):
        return new int[] {122};
      case (8360):
        return new int[] {114, 115};
      case (181):
        return new int[] {956};
      case (192):
        return new int[] {224};
      case (193):
        return new int[] {225};
      case (194):
        return new int[] {226};
      case (195):
        return new int[] {227};
      case (196):
        return new int[] {228};
      case (197):
        return new int[] {229};
      case (198):
        return new int[] {230};
      case (199):
        return new int[] {231};
      case (200):
        return new int[] {232};
      case (201):
        return new int[] {233};
      case (202):
        return new int[] {234};
      case (203):
        return new int[] {235};
      case (204):
        return new int[] {236};
      case (205):
        return new int[] {237};
      case (206):
        return new int[] {238};
      case (207):
        return new int[] {239};
      case (208):
        return new int[] {240};
      case (209):
        return new int[] {241};
      case (210):
        return new int[] {242};
      case (211):
        return new int[] {243};
      case (212):
        return new int[] {244};
      case (213):
        return new int[] {245};
      case (214):
        return new int[] {246};
      case (216):
        return new int[] {248};
      case (217):
        return new int[] {249};
      case (218):
        return new int[] {250};
      case (219):
        return new int[] {251};
      case (220):
        return new int[] {252};
      case (221):
        return new int[] {253};
      case (222):
        return new int[] {254};
      case (223):
        return new int[] {115, 115};
      case (256):
        return new int[] {257};
      case (258):
        return new int[] {259};
      case (8450):
        return new int[] {99};
      case (8451):
        return new int[] {176, 99};
      case (260):
        return new int[] {261};
      case (262):
        return new int[] {263};
      case (8455):
        return new int[] {603};
      case (264):
        return new int[] {265};
      case (8457):
        return new int[] {176, 102};
      case (266):
        return new int[] {267};
      case (8459):
        return new int[] {104};
      case (268):
        return new int[] {269};
      case (8460):
        return new int[] {104};
      case (8461):
        return new int[] {104};
      case (270):
        return new int[] {271};
      case (272):
        return new int[] {273};
      case (8464):
        return new int[] {105};
      case (8465):
        return new int[] {105};
      case (274):
        return new int[] {275};
      case (8466):
        return new int[] {108};
      case (276):
        return new int[] {277};
      case (8469):
        return new int[] {110};
      case (278):
        return new int[] {279};
      case (8470):
        return new int[] {110, 111};
      case (280):
        return new int[] {281};
      case (8473):
        return new int[] {112};
      case (282):
        return new int[] {283};
      case (8474):
        return new int[] {113};
      case (8475):
        return new int[] {114};
      case (284):
        return new int[] {285};
      case (8476):
        return new int[] {114};
      case (8477):
        return new int[] {114};
      case (286):
        return new int[] {287};
      case (288):
        return new int[] {289};
      case (8480):
        return new int[] {115, 109};
      case (8481):
        return new int[] {116, 101, 108};
      case (290):
        return new int[] {291};
      case (8482):
        return new int[] {116, 109};
      case (292):
        return new int[] {293};
      case (8484):
        return new int[] {122};
      case (294):
        return new int[] {295};
      case (8486):
        return new int[] {969};
      case (296):
        return new int[] {297};
      case (8488):
        return new int[] {122};
      case (298):
        return new int[] {299};
      case (8490):
        return new int[] {107};
      case (8491):
        return new int[] {229};
      case (300):
        return new int[] {301};
      case (8492):
        return new int[] {98};
      case (8493):
        return new int[] {99};
      case (302):
        return new int[] {303};
      case (304):
        return new int[] {105, 775};
      case (8496):
        return new int[] {101};
      case (8497):
        return new int[] {102};
      case (306):
        return new int[] {307};
      case (8499):
        return new int[] {109};
      case (308):
        return new int[] {309};
      case (310):
        return new int[] {311};
      case (313):
        return new int[] {314};
      case (315):
        return new int[] {316};
      case (317):
        return new int[] {318};
      case (8510):
        return new int[] {947};
      case (319):
        return new int[] {320};
      case (8511):
        return new int[] {960};
      case (321):
        return new int[] {322};
      case (323):
        return new int[] {324};
      case (325):
        return new int[] {326};
      case (8517):
        return new int[] {100};
      case (327):
        return new int[] {328};
      case (329):
        return new int[] {700, 110};
      case (330):
        return new int[] {331};
      case (332):
        return new int[] {333};
      case (334):
        return new int[] {335};
      case (336):
        return new int[] {337};
      case (338):
        return new int[] {339};
      case (340):
        return new int[] {341};
      case (342):
        return new int[] {343};
      case (344):
        return new int[] {345};
      case (346):
        return new int[] {347};
      case (348):
        return new int[] {349};
      case (350):
        return new int[] {351};
      case (352):
        return new int[] {353};
      case (8544):
        return new int[] {8560};
      case (8545):
        return new int[] {8561};
      case (354):
        return new int[] {355};
      case (8546):
        return new int[] {8562};
      case (8547):
        return new int[] {8563};
      case (356):
        return new int[] {357};
      case (8548):
        return new int[] {8564};
      case (8549):
        return new int[] {8565};
      case (358):
        return new int[] {359};
      case (8550):
        return new int[] {8566};
      case (8551):
        return new int[] {8567};
      case (360):
        return new int[] {361};
      case (8552):
        return new int[] {8568};
      case (8553):
        return new int[] {8569};
      case (362):
        return new int[] {363};
      case (8554):
        return new int[] {8570};
      case (8555):
        return new int[] {8571};
      case (364):
        return new int[] {365};
      case (8556):
        return new int[] {8572};
      case (8557):
        return new int[] {8573};
      case (366):
        return new int[] {367};
      case (8558):
        return new int[] {8574};
      case (8559):
        return new int[] {8575};
      case (368):
        return new int[] {369};
      case (370):
        return new int[] {371};
      case (372):
        return new int[] {373};
      case (374):
        return new int[] {375};
      case (376):
        return new int[] {255};
      case (377):
        return new int[] {378};
      case (379):
        return new int[] {380};
      case (381):
        return new int[] {382};
      case (383):
        return new int[] {115};
      case (385):
        return new int[] {595};
      case (386):
        return new int[] {387};
      case (388):
        return new int[] {389};
      case (390):
        return new int[] {596};
      case (391):
        return new int[] {392};
      case (393):
        return new int[] {598};
      case (394):
        return new int[] {599};
      case (395):
        return new int[] {396};
      case (398):
        return new int[] {477};
      case (399):
        return new int[] {601};
      case (400):
        return new int[] {603};
      case (401):
        return new int[] {402};
      case (403):
        return new int[] {608};
      case (404):
        return new int[] {611};
      case (406):
        return new int[] {617};
      case (407):
        return new int[] {616};
      case (408):
        return new int[] {409};
      case (412):
        return new int[] {623};
      case (413):
        return new int[] {626};
      case (415):
        return new int[] {629};
      case (416):
        return new int[] {417};
      case (418):
        return new int[] {419};
      case (420):
        return new int[] {421};
      case (422):
        return new int[] {640};
      case (423):
        return new int[] {424};
      case (425):
        return new int[] {643};
      case (428):
        return new int[] {429};
      case (430):
        return new int[] {648};
      case (431):
        return new int[] {432};
      case (433):
        return new int[] {650};
      case (434):
        return new int[] {651};
      case (435):
        return new int[] {436};
      case (437):
        return new int[] {438};
      case (439):
        return new int[] {658};
      case (440):
        return new int[] {441};
      case (444):
        return new int[] {445};
      case (452):
        return new int[] {454};
      case (453):
        return new int[] {454};
      case (455):
        return new int[] {457};
      case (456):
        return new int[] {457};
      case (458):
        return new int[] {460};
      case (459):
        return new int[] {460};
      case (461):
        return new int[] {462};
      case (463):
        return new int[] {464};
      case (465):
        return new int[] {466};
      case (467):
        return new int[] {468};
      case (469):
        return new int[] {470};
      case (471):
        return new int[] {472};
      case (473):
        return new int[] {474};
      case (475):
        return new int[] {476};
      case (478):
        return new int[] {479};
      case (480):
        return new int[] {481};
      case (482):
        return new int[] {483};
      case (484):
        return new int[] {485};
      case (486):
        return new int[] {487};
      case (488):
        return new int[] {489};
      case (490):
        return new int[] {491};
      case (492):
        return new int[] {493};
      case (494):
        return new int[] {495};
      case (496):
        return new int[] {106, 780};
      case (497):
        return new int[] {499};
      case (498):
        return new int[] {499};
      case (500):
        return new int[] {501};
      case (502):
        return new int[] {405};
      case (503):
        return new int[] {447};
      case (504):
        return new int[] {505};
      case (506):
        return new int[] {507};
      case (508):
        return new int[] {509};
      case (510):
        return new int[] {511};
      case (512):
        return new int[] {513};
      case (514):
        return new int[] {515};
      case (516):
        return new int[] {517};
      case (518):
        return new int[] {519};
      case (520):
        return new int[] {521};
      case (522):
        return new int[] {523};
      case (524):
        return new int[] {525};
      case (526):
        return new int[] {527};
      case (528):
        return new int[] {529};
      case (530):
        return new int[] {531};
      case (532):
        return new int[] {533};
      case (534):
        return new int[] {535};
      case (536):
        return new int[] {537};
      case (538):
        return new int[] {539};
      case (540):
        return new int[] {541};
      case (542):
        return new int[] {543};
      case (544):
        return new int[] {414};
      case (546):
        return new int[] {547};
      case (548):
        return new int[] {549};
      case (550):
        return new int[] {551};
      case (552):
        return new int[] {553};
      case (554):
        return new int[] {555};
      case (556):
        return new int[] {557};
      case (558):
        return new int[] {559};
      case (560):
        return new int[] {561};
      case (562):
        return new int[] {563};
      case (64256):
        return new int[] {102, 102};
      case (64257):
        return new int[] {102, 105};
      case (64258):
        return new int[] {102, 108};
      case (64259):
        return new int[] {102, 102, 105};
      case (64260):
        return new int[] {102, 102, 108};
      case (64261):
        return new int[] {115, 116};
      case (64262):
        return new int[] {115, 116};
      case (64275):
        return new int[] {1396, 1398};
      case (64276):
        return new int[] {1396, 1381};
      case (64277):
        return new int[] {1396, 1387};
      case (64278):
        return new int[] {1406, 1398};
      case (64279):
        return new int[] {1396, 1389};
      case (837):
        return new int[] {953};
      case (13169):
        return new int[] {104, 112, 97};
      case (13171):
        return new int[] {97, 117};
      case (13173):
        return new int[] {111, 118};
      case (890):
        return new int[] {32, 953};
      case (13184):
        return new int[] {112, 97};
      case (13185):
        return new int[] {110, 97};
      case (13186):
        return new int[] {956, 97};
      case (13187):
        return new int[] {109, 97};
      case (13188):
        return new int[] {107, 97};
      case (13189):
        return new int[] {107, 98};
      case (902):
        return new int[] {940};
      case (13190):
        return new int[] {109, 98};
      case (13191):
        return new int[] {103, 98};
      case (904):
        return new int[] {941};
      case (905):
        return new int[] {942};
      case (906):
        return new int[] {943};
      case (13194):
        return new int[] {112, 102};
      case (13195):
        return new int[] {110, 102};
      case (908):
        return new int[] {972};
      case (13196):
        return new int[] {956, 102};
      case (910):
        return new int[] {973};
      case (911):
        return new int[] {974};
      case (912):
        return new int[] {953, 776, 769};
      case (13200):
        return new int[] {104, 122};
      case (913):
        return new int[] {945};
      case (13201):
        return new int[] {107, 104, 122};
      case (914):
        return new int[] {946};
      case (13202):
        return new int[] {109, 104, 122};
      case (915):
        return new int[] {947};
      case (13203):
        return new int[] {103, 104, 122};
      case (916):
        return new int[] {948};
      case (13204):
        return new int[] {116, 104, 122};
      case (917):
        return new int[] {949};
      case (918):
        return new int[] {950};
      case (919):
        return new int[] {951};
      case (920):
        return new int[] {952};
      case (921):
        return new int[] {953};
      case (922):
        return new int[] {954};
      case (923):
        return new int[] {955};
      case (924):
        return new int[] {956};
      case (925):
        return new int[] {957};
      case (926):
        return new int[] {958};
      case (927):
        return new int[] {959};
      case (928):
        return new int[] {960};
      case (929):
        return new int[] {961};
      case (931):
        return new int[] {963};
      case (932):
        return new int[] {964};
      case (933):
        return new int[] {965};
      case (934):
        return new int[] {966};
      case (935):
        return new int[] {967};
      case (936):
        return new int[] {968};
      case (937):
        return new int[] {969};
      case (13225):
        return new int[] {112, 97};
      case (938):
        return new int[] {970};
      case (13226):
        return new int[] {107, 112, 97};
      case (939):
        return new int[] {971};
      case (13227):
        return new int[] {109, 112, 97};
      case (13228):
        return new int[] {103, 112, 97};
      case (944):
        return new int[] {965, 776, 769};
      case (13236):
        return new int[] {112, 118};
      case (13237):
        return new int[] {110, 118};
      case (13238):
        return new int[] {956, 118};
      case (13239):
        return new int[] {109, 118};
      case (13240):
        return new int[] {107, 118};
      case (13241):
        return new int[] {109, 118};
      case (13242):
        return new int[] {112, 119};
      case (13243):
        return new int[] {110, 119};
      case (13244):
        return new int[] {956, 119};
      case (13245):
        return new int[] {109, 119};
      case (13246):
        return new int[] {107, 119};
      case (13247):
        return new int[] {109, 119};
      case (13248):
        return new int[] {107, 969};
      case (13249):
        return new int[] {109, 969};
      case (962):
        return new int[] {963};
      case (13251):
        return new int[] {98, 113};
      case (13254):
        return new int[] {99, 8725, 107, 103};
      case (13255):
        return new int[] {99, 111, 46};
      case (13256):
        return new int[] {100, 98};
      case (13257):
        return new int[] {103, 121};
      case (13259):
        return new int[] {104, 112};
      case (13261):
        return new int[] {107, 107};
      case (13262):
        return new int[] {107, 109};
      case (976):
        return new int[] {946};
      case (977):
        return new int[] {952};
      case (978):
        return new int[] {965};
      case (979):
        return new int[] {973};
      case (980):
        return new int[] {971};
      case (981):
        return new int[] {966};
      case (982):
        return new int[] {960};
      case (13271):
        return new int[] {112, 104};
      case (984):
        return new int[] {985};
      case (13273):
        return new int[] {112, 112, 109};
      case (986):
        return new int[] {987};
      case (13274):
        return new int[] {112, 114};
      case (988):
        return new int[] {989};
      case (13276):
        return new int[] {115, 118};
      case (13277):
        return new int[] {119, 98};
      case (990):
        return new int[] {991};
      case (992):
        return new int[] {993};
      case (994):
        return new int[] {995};
      case (996):
        return new int[] {997};
      case (998):
        return new int[] {999};
      case (1000):
        return new int[] {1001};
      case (1002):
        return new int[] {1003};
      case (1004):
        return new int[] {1005};
      case (1006):
        return new int[] {1007};
      case (1008):
        return new int[] {954};
      case (1009):
        return new int[] {961};
      case (1010):
        return new int[] {963};
      case (1012):
        return new int[] {952};
      case (1013):
        return new int[] {949};
      case (1024):
        return new int[] {1104};
      case (66561):
        return new int[] {66601};
      case (119809):
        return new int[] {98};
      case (1025):
        return new int[] {1105};
      case (66560):
        return new int[] {66600};
      case (119808):
        return new int[] {97};
      case (1026):
        return new int[] {1106};
      case (66563):
        return new int[] {66603};
      case (119811):
        return new int[] {100};
      case (1027):
        return new int[] {1107};
      case (66562):
        return new int[] {66602};
      case (119810):
        return new int[] {99};
      case (1028):
        return new int[] {1108};
      case (66565):
        return new int[] {66605};
      case (119813):
        return new int[] {102};
      case (1029):
        return new int[] {1109};
      case (66564):
        return new int[] {66604};
      case (119812):
        return new int[] {101};
      case (1030):
        return new int[] {1110};
      case (66567):
        return new int[] {66607};
      case (119815):
        return new int[] {104};
      case (1031):
        return new int[] {1111};
      case (66566):
        return new int[] {66606};
      case (119814):
        return new int[] {103};
      case (1032):
        return new int[] {1112};
      case (66569):
        return new int[] {66609};
      case (119817):
        return new int[] {106};
      case (1033):
        return new int[] {1113};
      case (66568):
        return new int[] {66608};
      case (119816):
        return new int[] {105};
      case (1034):
        return new int[] {1114};
      case (66571):
        return new int[] {66611};
      case (119819):
        return new int[] {108};
      case (1035):
        return new int[] {1115};
      case (66570):
        return new int[] {66610};
      case (119818):
        return new int[] {107};
      case (1036):
        return new int[] {1116};
      case (66573):
        return new int[] {66613};
      case (119821):
        return new int[] {110};
      case (1037):
        return new int[] {1117};
      case (66572):
        return new int[] {66612};
      case (119820):
        return new int[] {109};
      case (1038):
        return new int[] {1118};
      case (66575):
        return new int[] {66615};
      case (119823):
        return new int[] {112};
      case (1039):
        return new int[] {1119};
      case (66574):
        return new int[] {66614};
      case (119822):
        return new int[] {111};
      case (1040):
        return new int[] {1072};
      case (66577):
        return new int[] {66617};
      case (119825):
        return new int[] {114};
      case (1041):
        return new int[] {1073};
      case (66576):
        return new int[] {66616};
      case (119824):
        return new int[] {113};
      case (1042):
        return new int[] {1074};
      case (66579):
        return new int[] {66619};
      case (119827):
        return new int[] {116};
      case (1043):
        return new int[] {1075};
      case (66578):
        return new int[] {66618};
      case (119826):
        return new int[] {115};
      case (1044):
        return new int[] {1076};
      case (66581):
        return new int[] {66621};
      case (119829):
        return new int[] {118};
      case (1045):
        return new int[] {1077};
      case (66580):
        return new int[] {66620};
      case (119828):
        return new int[] {117};
      case (1046):
        return new int[] {1078};
      case (66583):
        return new int[] {66623};
      case (119831):
        return new int[] {120};
      case (1047):
        return new int[] {1079};
      case (66582):
        return new int[] {66622};
      case (119830):
        return new int[] {119};
      case (1048):
        return new int[] {1080};
      case (66585):
        return new int[] {66625};
      case (119833):
        return new int[] {122};
      case (1049):
        return new int[] {1081};
      case (66584):
        return new int[] {66624};
      case (119832):
        return new int[] {121};
      case (1050):
        return new int[] {1082};
      case (66587):
        return new int[] {66627};
      case (1051):
        return new int[] {1083};
      case (66586):
        return new int[] {66626};
      case (1052):
        return new int[] {1084};
      case (66589):
        return new int[] {66629};
      case (1053):
        return new int[] {1085};
      case (66588):
        return new int[] {66628};
      case (1054):
        return new int[] {1086};
      case (66591):
        return new int[] {66631};
      case (1055):
        return new int[] {1087};
      case (66590):
        return new int[] {66630};
      case (1056):
        return new int[] {1088};
      case (66593):
        return new int[] {66633};
      case (1057):
        return new int[] {1089};
      case (66592):
        return new int[] {66632};
      case (1058):
        return new int[] {1090};
      case (66595):
        return new int[] {66635};
      case (1059):
        return new int[] {1091};
      case (66594):
        return new int[] {66634};
      case (1060):
        return new int[] {1092};
      case (66597):
        return new int[] {66637};
      case (1061):
        return new int[] {1093};
      case (66596):
        return new int[] {66636};
      case (1062):
        return new int[] {1094};
      case (1063):
        return new int[] {1095};
      case (1064):
        return new int[] {1096};
      case (1065):
        return new int[] {1097};
      case (1066):
        return new int[] {1098};
      case (1067):
        return new int[] {1099};
      case (1068):
        return new int[] {1100};
      case (1069):
        return new int[] {1101};
      case (1070):
        return new int[] {1102};
      case (1071):
        return new int[] {1103};
      case (119861):
        return new int[] {98};
      case (119860):
        return new int[] {97};
      case (119863):
        return new int[] {100};
      case (119862):
        return new int[] {99};
      case (119865):
        return new int[] {102};
      case (119864):
        return new int[] {101};
      case (119867):
        return new int[] {104};
      case (119866):
        return new int[] {103};
      case (119869):
        return new int[] {106};
      case (119868):
        return new int[] {105};
      case (119871):
        return new int[] {108};
      case (119870):
        return new int[] {107};
      case (119873):
        return new int[] {110};
      case (119872):
        return new int[] {109};
      case (119875):
        return new int[] {112};
      case (119874):
        return new int[] {111};
      case (119877):
        return new int[] {114};
      case (119876):
        return new int[] {113};
      case (119879):
        return new int[] {116};
      case (119878):
        return new int[] {115};
      case (119881):
        return new int[] {118};
      case (119880):
        return new int[] {117};
      case (119883):
        return new int[] {120};
      case (119882):
        return new int[] {119};
      case (119885):
        return new int[] {122};
      case (119884):
        return new int[] {121};
      case (1120):
        return new int[] {1121};
      case (1122):
        return new int[] {1123};
      case (1124):
        return new int[] {1125};
      case (1126):
        return new int[] {1127};
      case (1128):
        return new int[] {1129};
      case (119913):
        return new int[] {98};
      case (119912):
        return new int[] {97};
      case (1130):
        return new int[] {1131};
      case (119915):
        return new int[] {100};
      case (119914):
        return new int[] {99};
      case (1132):
        return new int[] {1133};
      case (119917):
        return new int[] {102};
      case (119916):
        return new int[] {101};
      case (1134):
        return new int[] {1135};
      case (119919):
        return new int[] {104};
      case (119918):
        return new int[] {103};
      case (1136):
        return new int[] {1137};
      case (119921):
        return new int[] {106};
      case (119920):
        return new int[] {105};
      case (1138):
        return new int[] {1139};
      case (119923):
        return new int[] {108};
      case (119922):
        return new int[] {107};
      case (1140):
        return new int[] {1141};
      case (119925):
        return new int[] {110};
      case (119924):
        return new int[] {109};
      case (1142):
        return new int[] {1143};
      case (119927):
        return new int[] {112};
      case (119926):
        return new int[] {111};
      case (1144):
        return new int[] {1145};
      case (119929):
        return new int[] {114};
      case (119928):
        return new int[] {113};
      case (1146):
        return new int[] {1147};
      case (119931):
        return new int[] {116};
      case (119930):
        return new int[] {115};
      case (1148):
        return new int[] {1149};
      case (119933):
        return new int[] {118};
      case (119932):
        return new int[] {117};
      case (1150):
        return new int[] {1151};
      case (119935):
        return new int[] {120};
      case (119934):
        return new int[] {119};
      case (1152):
        return new int[] {1153};
      case (119937):
        return new int[] {122};
      case (119936):
        return new int[] {121};
      case (1162):
        return new int[] {1163};
      case (1164):
        return new int[] {1165};
      case (1166):
        return new int[] {1167};
      case (1168):
        return new int[] {1169};
      case (1170):
        return new int[] {1171};
      case (1172):
        return new int[] {1173};
      case (1174):
        return new int[] {1175};
      case (1176):
        return new int[] {1177};
      case (1178):
        return new int[] {1179};
      case (1180):
        return new int[] {1181};
      case (119964):
        return new int[] {97};
      case (1182):
        return new int[] {1183};
      case (119967):
        return new int[] {100};
      case (119966):
        return new int[] {99};
      case (1184):
        return new int[] {1185};
      case (1186):
        return new int[] {1187};
      case (119970):
        return new int[] {103};
      case (1188):
        return new int[] {1189};
      case (119973):
        return new int[] {106};
      case (1190):
        return new int[] {1191};
      case (119974):
        return new int[] {107};
      case (1192):
        return new int[] {1193};
      case (119977):
        return new int[] {110};
      case (1194):
        return new int[] {1195};
      case (119979):
        return new int[] {112};
      case (119978):
        return new int[] {111};
      case (1196):
        return new int[] {1197};
      case (119980):
        return new int[] {113};
      case (1198):
        return new int[] {1199};
      case (119983):
        return new int[] {116};
      case (119982):
        return new int[] {115};
      case (1200):
        return new int[] {1201};
      case (119985):
        return new int[] {118};
      case (119984):
        return new int[] {117};
      case (1202):
        return new int[] {1203};
      case (119987):
        return new int[] {120};
      case (119986):
        return new int[] {119};
      case (1204):
        return new int[] {1205};
      case (119989):
        return new int[] {122};
      case (119988):
        return new int[] {121};
      case (1206):
        return new int[] {1207};
      case (9398):
        return new int[] {9424};
      case (9399):
        return new int[] {9425};
      case (1208):
        return new int[] {1209};
      case (9400):
        return new int[] {9426};
      case (9401):
        return new int[] {9427};
      case (1210):
        return new int[] {1211};
      case (9402):
        return new int[] {9428};
      case (9403):
        return new int[] {9429};
      case (1212):
        return new int[] {1213};
      case (9404):
        return new int[] {9430};
      case (9405):
        return new int[] {9431};
      case (1214):
        return new int[] {1215};
      case (9406):
        return new int[] {9432};
      case (9407):
        return new int[] {9433};
      case (9408):
        return new int[] {9434};
      case (1217):
        return new int[] {1218};
      case (9409):
        return new int[] {9435};
      case (9410):
        return new int[] {9436};
      case (1219):
        return new int[] {1220};
      case (9411):
        return new int[] {9437};
      case (9412):
        return new int[] {9438};
      case (1221):
        return new int[] {1222};
      case (9413):
        return new int[] {9439};
      case (9414):
        return new int[] {9440};
      case (1223):
        return new int[] {1224};
      case (9415):
        return new int[] {9441};
      case (9416):
        return new int[] {9442};
      case (1225):
        return new int[] {1226};
      case (9417):
        return new int[] {9443};
      case (9418):
        return new int[] {9444};
      case (1227):
        return new int[] {1228};
      case (9419):
        return new int[] {9445};
      case (9420):
        return new int[] {9446};
      case (1229):
        return new int[] {1230};
      case (9421):
        return new int[] {9447};
      case (9422):
        return new int[] {9448};
      case (9423):
        return new int[] {9449};
      case (1232):
        return new int[] {1233};
      case (120017):
        return new int[] {98};
      case (120016):
        return new int[] {97};
      case (1234):
        return new int[] {1235};
      case (120019):
        return new int[] {100};
      case (120018):
        return new int[] {99};
      case (1236):
        return new int[] {1237};
      case (120021):
        return new int[] {102};
      case (120020):
        return new int[] {101};
      case (1238):
        return new int[] {1239};
      case (120023):
        return new int[] {104};
      case (120022):
        return new int[] {103};
      case (1240):
        return new int[] {1241};
      case (120025):
        return new int[] {106};
      case (120024):
        return new int[] {105};
      case (1242):
        return new int[] {1243};
      case (120027):
        return new int[] {108};
      case (120026):
        return new int[] {107};
      case (1244):
        return new int[] {1245};
      case (120029):
        return new int[] {110};
      case (120028):
        return new int[] {109};
      case (1246):
        return new int[] {1247};
      case (120031):
        return new int[] {112};
      case (120030):
        return new int[] {111};
      case (1248):
        return new int[] {1249};
      case (120033):
        return new int[] {114};
      case (120032):
        return new int[] {113};
      case (1250):
        return new int[] {1251};
      case (120035):
        return new int[] {116};
      case (120034):
        return new int[] {115};
      case (1252):
        return new int[] {1253};
      case (120037):
        return new int[] {118};
      case (120036):
        return new int[] {117};
      case (1254):
        return new int[] {1255};
      case (120039):
        return new int[] {120};
      case (120038):
        return new int[] {119};
      case (1256):
        return new int[] {1257};
      case (120041):
        return new int[] {122};
      case (120040):
        return new int[] {121};
      case (1258):
        return new int[] {1259};
      case (1260):
        return new int[] {1261};
      case (1262):
        return new int[] {1263};
      case (1264):
        return new int[] {1265};
      case (1266):
        return new int[] {1267};
      case (1268):
        return new int[] {1269};
      case (1272):
        return new int[] {1273};
      case (1280):
        return new int[] {1281};
      case (1282):
        return new int[] {1283};
      case (1284):
        return new int[] {1285};
      case (120069):
        return new int[] {98};
      case (120068):
        return new int[] {97};
      case (1286):
        return new int[] {1287};
      case (120071):
        return new int[] {100};
      case (1288):
        return new int[] {1289};
      case (120073):
        return new int[] {102};
      case (120072):
        return new int[] {101};
      case (1290):
        return new int[] {1291};
      case (120074):
        return new int[] {103};
      case (1292):
        return new int[] {1293};
      case (120077):
        return new int[] {106};
      case (1294):
        return new int[] {1295};
      case (120079):
        return new int[] {108};
      case (120078):
        return new int[] {107};
      case (120081):
        return new int[] {110};
      case (120080):
        return new int[] {109};
      case (120083):
        return new int[] {112};
      case (120082):
        return new int[] {111};
      case (120084):
        return new int[] {113};
      case (120087):
        return new int[] {116};
      case (120086):
        return new int[] {115};
      case (120089):
        return new int[] {118};
      case (120088):
        return new int[] {117};
      case (120091):
        return new int[] {120};
      case (120090):
        return new int[] {119};
      case (120092):
        return new int[] {121};
      case (1329):
        return new int[] {1377};
      case (1330):
        return new int[] {1378};
      case (1331):
        return new int[] {1379};
      case (1332):
        return new int[] {1380};
      case (1333):
        return new int[] {1381};
      case (1334):
        return new int[] {1382};
      case (1335):
        return new int[] {1383};
      case (1336):
        return new int[] {1384};
      case (120121):
        return new int[] {98};
      case (1337):
        return new int[] {1385};
      case (120120):
        return new int[] {97};
      case (1338):
        return new int[] {1386};
      case (120123):
        return new int[] {100};
      case (1339):
        return new int[] {1387};
      case (1340):
        return new int[] {1388};
      case (120125):
        return new int[] {102};
      case (1341):
        return new int[] {1389};
      case (120124):
        return new int[] {101};
      case (1342):
        return new int[] {1390};
      case (1343):
        return new int[] {1391};
      case (120126):
        return new int[] {103};
      case (1344):
        return new int[] {1392};
      case (120129):
        return new int[] {106};
      case (1345):
        return new int[] {1393};
      case (120128):
        return new int[] {105};
      case (1346):
        return new int[] {1394};
      case (120131):
        return new int[] {108};
      case (1347):
        return new int[] {1395};
      case (120130):
        return new int[] {107};
      case (1348):
        return new int[] {1396};
      case (1349):
        return new int[] {1397};
      case (120132):
        return new int[] {109};
      case (1350):
        return new int[] {1398};
      case (1351):
        return new int[] {1399};
      case (120134):
        return new int[] {111};
      case (1352):
        return new int[] {1400};
      case (1353):
        return new int[] {1401};
      case (1354):
        return new int[] {1402};
      case (120139):
        return new int[] {116};
      case (1355):
        return new int[] {1403};
      case (120138):
        return new int[] {115};
      case (1356):
        return new int[] {1404};
      case (120141):
        return new int[] {118};
      case (1357):
        return new int[] {1405};
      case (120140):
        return new int[] {117};
      case (1358):
        return new int[] {1406};
      case (120143):
        return new int[] {120};
      case (1359):
        return new int[] {1407};
      case (120142):
        return new int[] {119};
      case (1360):
        return new int[] {1408};
      case (1361):
        return new int[] {1409};
      case (120144):
        return new int[] {121};
      case (1362):
        return new int[] {1410};
      case (1363):
        return new int[] {1411};
      case (1364):
        return new int[] {1412};
      case (1365):
        return new int[] {1413};
      case (1366):
        return new int[] {1414};
      case (120173):
        return new int[] {98};
      case (120172):
        return new int[] {97};
      case (120175):
        return new int[] {100};
      case (120174):
        return new int[] {99};
      case (120177):
        return new int[] {102};
      case (120176):
        return new int[] {101};
      case (120179):
        return new int[] {104};
      case (120178):
        return new int[] {103};
      case (120181):
        return new int[] {106};
      case (120180):
        return new int[] {105};
      case (120183):
        return new int[] {108};
      case (120182):
        return new int[] {107};
      case (120185):
        return new int[] {110};
      case (120184):
        return new int[] {109};
      case (120187):
        return new int[] {112};
      case (120186):
        return new int[] {111};
      case (120189):
        return new int[] {114};
      case (120188):
        return new int[] {113};
      case (120191):
        return new int[] {116};
      case (120190):
        return new int[] {115};
      case (120193):
        return new int[] {118};
      case (120192):
        return new int[] {117};
      case (120195):
        return new int[] {120};
      case (120194):
        return new int[] {119};
      case (120197):
        return new int[] {122};
      case (120196):
        return new int[] {121};
      case (1415):
        return new int[] {1381, 1410};
      case (120225):
        return new int[] {98};
      case (120224):
        return new int[] {97};
      case (120227):
        return new int[] {100};
      case (120226):
        return new int[] {99};
      case (120229):
        return new int[] {102};
      case (120228):
        return new int[] {101};
      case (120231):
        return new int[] {104};
      case (120230):
        return new int[] {103};
      case (120233):
        return new int[] {106};
      case (120232):
        return new int[] {105};
      case (120235):
        return new int[] {108};
      case (120234):
        return new int[] {107};
      case (120237):
        return new int[] {110};
      case (120236):
        return new int[] {109};
      case (120239):
        return new int[] {112};
      case (120238):
        return new int[] {111};
      case (120241):
        return new int[] {114};
      case (120240):
        return new int[] {113};
      case (120243):
        return new int[] {116};
      case (120242):
        return new int[] {115};
      case (120245):
        return new int[] {118};
      case (120244):
        return new int[] {117};
      case (120247):
        return new int[] {120};
      case (120246):
        return new int[] {119};
      case (120249):
        return new int[] {122};
      case (120248):
        return new int[] {121};
      case (120277):
        return new int[] {98};
      case (120276):
        return new int[] {97};
      case (120279):
        return new int[] {100};
      case (120278):
        return new int[] {99};
      case (120281):
        return new int[] {102};
      case (120280):
        return new int[] {101};
      case (120283):
        return new int[] {104};
      case (120282):
        return new int[] {103};
      case (120285):
        return new int[] {106};
      case (120284):
        return new int[] {105};
      case (120287):
        return new int[] {108};
      case (120286):
        return new int[] {107};
      case (120289):
        return new int[] {110};
      case (120288):
        return new int[] {109};
      case (120291):
        return new int[] {112};
      case (120290):
        return new int[] {111};
      case (120293):
        return new int[] {114};
      case (120292):
        return new int[] {113};
      case (120295):
        return new int[] {116};
      case (120294):
        return new int[] {115};
      case (120297):
        return new int[] {118};
      case (120296):
        return new int[] {117};
      case (120299):
        return new int[] {120};
      case (120298):
        return new int[] {119};
      case (120301):
        return new int[] {122};
      case (120300):
        return new int[] {121};
      case (7680):
        return new int[] {7681};
      case (7682):
        return new int[] {7683};
      case (7684):
        return new int[] {7685};
      case (7686):
        return new int[] {7687};
      case (7688):
        return new int[] {7689};
      case (120329):
        return new int[] {98};
      case (120328):
        return new int[] {97};
      case (7690):
        return new int[] {7691};
      case (120331):
        return new int[] {100};
      case (120330):
        return new int[] {99};
      case (7692):
        return new int[] {7693};
      case (120333):
        return new int[] {102};
      case (120332):
        return new int[] {101};
      case (7694):
        return new int[] {7695};
      case (120335):
        return new int[] {104};
      case (120334):
        return new int[] {103};
      case (7696):
        return new int[] {7697};
      case (120337):
        return new int[] {106};
      case (120336):
        return new int[] {105};
      case (7698):
        return new int[] {7699};
      case (120339):
        return new int[] {108};
      case (120338):
        return new int[] {107};
      case (7700):
        return new int[] {7701};
      case (120341):
        return new int[] {110};
      case (120340):
        return new int[] {109};
      case (7702):
        return new int[] {7703};
      case (120343):
        return new int[] {112};
      case (120342):
        return new int[] {111};
      case (7704):
        return new int[] {7705};
      case (120345):
        return new int[] {114};
      case (120344):
        return new int[] {113};
      case (7706):
        return new int[] {7707};
      case (120347):
        return new int[] {116};
      case (120346):
        return new int[] {115};
      case (7708):
        return new int[] {7709};
      case (120349):
        return new int[] {118};
      case (120348):
        return new int[] {117};
      case (7710):
        return new int[] {7711};
      case (120351):
        return new int[] {120};
      case (120350):
        return new int[] {119};
      case (7712):
        return new int[] {7713};
      case (120353):
        return new int[] {122};
      case (120352):
        return new int[] {121};
      case (7714):
        return new int[] {7715};
      case (7716):
        return new int[] {7717};
      case (7718):
        return new int[] {7719};
      case (7720):
        return new int[] {7721};
      case (7722):
        return new int[] {7723};
      case (7724):
        return new int[] {7725};
      case (7726):
        return new int[] {7727};
      case (7728):
        return new int[] {7729};
      case (7730):
        return new int[] {7731};
      case (7732):
        return new int[] {7733};
      case (7734):
        return new int[] {7735};
      case (7736):
        return new int[] {7737};
      case (7738):
        return new int[] {7739};
      case (7740):
        return new int[] {7741};
      case (120381):
        return new int[] {98};
      case (120380):
        return new int[] {97};
      case (7742):
        return new int[] {7743};
      case (120383):
        return new int[] {100};
      case (120382):
        return new int[] {99};
      case (7744):
        return new int[] {7745};
      case (120385):
        return new int[] {102};
      case (120384):
        return new int[] {101};
      case (7746):
        return new int[] {7747};
      case (120387):
        return new int[] {104};
      case (120386):
        return new int[] {103};
      case (7748):
        return new int[] {7749};
      case (120389):
        return new int[] {106};
      case (120388):
        return new int[] {105};
      case (7750):
        return new int[] {7751};
      case (120391):
        return new int[] {108};
      case (120390):
        return new int[] {107};
      case (7752):
        return new int[] {7753};
      case (120393):
        return new int[] {110};
      case (120392):
        return new int[] {109};
      case (7754):
        return new int[] {7755};
      case (120395):
        return new int[] {112};
      case (120394):
        return new int[] {111};
      case (7756):
        return new int[] {7757};
      case (120397):
        return new int[] {114};
      case (120396):
        return new int[] {113};
      case (7758):
        return new int[] {7759};
      case (120399):
        return new int[] {116};
      case (120398):
        return new int[] {115};
      case (7760):
        return new int[] {7761};
      case (120401):
        return new int[] {118};
      case (120400):
        return new int[] {117};
      case (7762):
        return new int[] {7763};
      case (120403):
        return new int[] {120};
      case (120402):
        return new int[] {119};
      case (7764):
        return new int[] {7765};
      case (120405):
        return new int[] {122};
      case (120404):
        return new int[] {121};
      case (7766):
        return new int[] {7767};
      case (7768):
        return new int[] {7769};
      case (7770):
        return new int[] {7771};
      case (7772):
        return new int[] {7773};
      case (7774):
        return new int[] {7775};
      case (7776):
        return new int[] {7777};
      case (7778):
        return new int[] {7779};
      case (7780):
        return new int[] {7781};
      case (7782):
        return new int[] {7783};
      case (7784):
        return new int[] {7785};
      case (7786):
        return new int[] {7787};
      case (7788):
        return new int[] {7789};
      case (7790):
        return new int[] {7791};
      case (7792):
        return new int[] {7793};
      case (120433):
        return new int[] {98};
      case (120432):
        return new int[] {97};
      case (7794):
        return new int[] {7795};
      case (120435):
        return new int[] {100};
      case (120434):
        return new int[] {99};
      case (7796):
        return new int[] {7797};
      case (120437):
        return new int[] {102};
      case (120436):
        return new int[] {101};
      case (7798):
        return new int[] {7799};
      case (120439):
        return new int[] {104};
      case (120438):
        return new int[] {103};
      case (7800):
        return new int[] {7801};
      case (120441):
        return new int[] {106};
      case (120440):
        return new int[] {105};
      case (7802):
        return new int[] {7803};
      case (120443):
        return new int[] {108};
      case (120442):
        return new int[] {107};
      case (7804):
        return new int[] {7805};
      case (120445):
        return new int[] {110};
      case (120444):
        return new int[] {109};
      case (7806):
        return new int[] {7807};
      case (120447):
        return new int[] {112};
      case (120446):
        return new int[] {111};
      case (7808):
        return new int[] {7809};
      case (120449):
        return new int[] {114};
      case (120448):
        return new int[] {113};
      case (7810):
        return new int[] {7811};
      case (120451):
        return new int[] {116};
      case (120450):
        return new int[] {115};
      case (7812):
        return new int[] {7813};
      case (120453):
        return new int[] {118};
      case (120452):
        return new int[] {117};
      case (7814):
        return new int[] {7815};
      case (120455):
        return new int[] {120};
      case (120454):
        return new int[] {119};
      case (7816):
        return new int[] {7817};
      case (120457):
        return new int[] {122};
      case (120456):
        return new int[] {121};
      case (7818):
        return new int[] {7819};
      case (7820):
        return new int[] {7821};
      case (7822):
        return new int[] {7823};
      case (7824):
        return new int[] {7825};
      case (7826):
        return new int[] {7827};
      case (7828):
        return new int[] {7829};
      case (7830):
        return new int[] {104, 817};
      case (7831):
        return new int[] {116, 776};
      case (7832):
        return new int[] {119, 778};
      case (7833):
        return new int[] {121, 778};
      case (7834):
        return new int[] {97, 702};
      case (7835):
        return new int[] {7777};
      case (7840):
        return new int[] {7841};
      case (7842):
        return new int[] {7843};
      case (7844):
        return new int[] {7845};
      case (7846):
        return new int[] {7847};
      case (7848):
        return new int[] {7849};
      case (120489):
        return new int[] {946};
      case (120488):
        return new int[] {945};
      case (7850):
        return new int[] {7851};
      case (120491):
        return new int[] {948};
      case (120490):
        return new int[] {947};
      case (7852):
        return new int[] {7853};
      case (120493):
        return new int[] {950};
      case (120492):
        return new int[] {949};
      case (7854):
        return new int[] {7855};
      case (120495):
        return new int[] {952};
      case (120494):
        return new int[] {951};
      case (7856):
        return new int[] {7857};
      case (120497):
        return new int[] {954};
      case (120496):
        return new int[] {953};
      case (7858):
        return new int[] {7859};
      case (120499):
        return new int[] {956};
      case (120498):
        return new int[] {955};
      case (7860):
        return new int[] {7861};
      case (120501):
        return new int[] {958};
      case (120500):
        return new int[] {957};
      case (7862):
        return new int[] {7863};
      case (120503):
        return new int[] {960};
      case (120502):
        return new int[] {959};
      case (7864):
        return new int[] {7865};
      case (120505):
        return new int[] {952};
      case (120504):
        return new int[] {961};
      case (7866):
        return new int[] {7867};
      case (120507):
        return new int[] {964};
      case (120506):
        return new int[] {963};
      case (7868):
        return new int[] {7869};
      case (120509):
        return new int[] {966};
      case (120508):
        return new int[] {965};
      case (7870):
        return new int[] {7871};
      case (120511):
        return new int[] {968};
      case (120510):
        return new int[] {967};
      case (7872):
        return new int[] {7873};
      case (120512):
        return new int[] {969};
      case (7874):
        return new int[] {7875};
      case (7876):
        return new int[] {7877};
      case (7878):
        return new int[] {7879};
      case (7880):
        return new int[] {7881};
      case (7882):
        return new int[] {7883};
      case (7884):
        return new int[] {7885};
      case (7886):
        return new int[] {7887};
      case (7888):
        return new int[] {7889};
      case (7890):
        return new int[] {7891};
      case (120531):
        return new int[] {963};
      case (7892):
        return new int[] {7893};
      case (7894):
        return new int[] {7895};
      case (7896):
        return new int[] {7897};
      case (7898):
        return new int[] {7899};
      case (7900):
        return new int[] {7901};
      case (7902):
        return new int[] {7903};
      case (7904):
        return new int[] {7905};
      case (7906):
        return new int[] {7907};
      case (120547):
        return new int[] {946};
      case (120546):
        return new int[] {945};
      case (7908):
        return new int[] {7909};
      case (120549):
        return new int[] {948};
      case (120548):
        return new int[] {947};
      case (7910):
        return new int[] {7911};
      case (120551):
        return new int[] {950};
      case (120550):
        return new int[] {949};
      case (7912):
        return new int[] {7913};
      case (120553):
        return new int[] {952};
      case (120552):
        return new int[] {951};
      case (7914):
        return new int[] {7915};
      case (120555):
        return new int[] {954};
      case (120554):
        return new int[] {953};
      case (7916):
        return new int[] {7917};
      case (120557):
        return new int[] {956};
      case (120556):
        return new int[] {955};
      case (7918):
        return new int[] {7919};
      case (120559):
        return new int[] {958};
      case (120558):
        return new int[] {957};
      case (7920):
        return new int[] {7921};
      case (120561):
        return new int[] {960};
      case (120560):
        return new int[] {959};
      case (7922):
        return new int[] {7923};
      case (120563):
        return new int[] {952};
      case (120562):
        return new int[] {961};
      case (7924):
        return new int[] {7925};
      case (120565):
        return new int[] {964};
      case (120564):
        return new int[] {963};
      case (7926):
        return new int[] {7927};
      case (120567):
        return new int[] {966};
      case (120566):
        return new int[] {965};
      case (7928):
        return new int[] {7929};
      case (120569):
        return new int[] {968};
      case (120568):
        return new int[] {967};
      case (120570):
        return new int[] {969};
      case (7944):
        return new int[] {7936};
      case (7945):
        return new int[] {7937};
      case (7946):
        return new int[] {7938};
      case (7947):
        return new int[] {7939};
      case (7948):
        return new int[] {7940};
      case (120589):
        return new int[] {963};
      case (7949):
        return new int[] {7941};
      case (7950):
        return new int[] {7942};
      case (7951):
        return new int[] {7943};
      case (7960):
        return new int[] {7952};
      case (7961):
        return new int[] {7953};
      case (7962):
        return new int[] {7954};
      case (7963):
        return new int[] {7955};
      case (7964):
        return new int[] {7956};
      case (120605):
        return new int[] {946};
      case (7965):
        return new int[] {7957};
      case (120604):
        return new int[] {945};
      case (120607):
        return new int[] {948};
      case (120606):
        return new int[] {947};
      case (120609):
        return new int[] {950};
      case (65313):
        return new int[] {65345};
      case (120608):
        return new int[] {949};
      case (65314):
        return new int[] {65346};
      case (120611):
        return new int[] {952};
      case (65315):
        return new int[] {65347};
      case (120610):
        return new int[] {951};
      case (65316):
        return new int[] {65348};
      case (120613):
        return new int[] {954};
      case (65317):
        return new int[] {65349};
      case (120612):
        return new int[] {953};
      case (65318):
        return new int[] {65350};
      case (120615):
        return new int[] {956};
      case (65319):
        return new int[] {65351};
      case (120614):
        return new int[] {955};
      case (7976):
        return new int[] {7968};
      case (65320):
        return new int[] {65352};
      case (120617):
        return new int[] {958};
      case (7977):
        return new int[] {7969};
      case (65321):
        return new int[] {65353};
      case (120616):
        return new int[] {957};
      case (7978):
        return new int[] {7970};
      case (65322):
        return new int[] {65354};
      case (120619):
        return new int[] {960};
      case (7979):
        return new int[] {7971};
      case (65323):
        return new int[] {65355};
      case (120618):
        return new int[] {959};
      case (7980):
        return new int[] {7972};
      case (65324):
        return new int[] {65356};
      case (120621):
        return new int[] {952};
      case (7981):
        return new int[] {7973};
      case (65325):
        return new int[] {65357};
      case (120620):
        return new int[] {961};
      case (7982):
        return new int[] {7974};
      case (65326):
        return new int[] {65358};
      case (120623):
        return new int[] {964};
      case (7983):
        return new int[] {7975};
      case (65327):
        return new int[] {65359};
      case (120622):
        return new int[] {963};
      case (65328):
        return new int[] {65360};
      case (120625):
        return new int[] {966};
      case (65329):
        return new int[] {65361};
      case (120624):
        return new int[] {965};
      case (65330):
        return new int[] {65362};
      case (120627):
        return new int[] {968};
      case (65331):
        return new int[] {65363};
      case (120626):
        return new int[] {967};
      case (65332):
        return new int[] {65364};
      case (65333):
        return new int[] {65365};
      case (120628):
        return new int[] {969};
      case (65334):
        return new int[] {65366};
      case (65335):
        return new int[] {65367};
      case (7992):
        return new int[] {7984};
      case (65336):
        return new int[] {65368};
      case (7993):
        return new int[] {7985};
      case (65337):
        return new int[] {65369};
      case (7994):
        return new int[] {7986};
      case (65338):
        return new int[] {65370};
      case (7995):
        return new int[] {7987};
      case (7996):
        return new int[] {7988};
      case (7997):
        return new int[] {7989};
      case (7998):
        return new int[] {7990};
      case (7999):
        return new int[] {7991};
      case (120647):
        return new int[] {963};
      case (8008):
        return new int[] {8000};
      case (8009):
        return new int[] {8001};
      case (8010):
        return new int[] {8002};
      case (8011):
        return new int[] {8003};
      case (8012):
        return new int[] {8004};
      case (8013):
        return new int[] {8005};
      case (8016):
        return new int[] {965, 787};
      case (8018):
        return new int[] {965, 787, 768};
      case (8020):
        return new int[] {965, 787, 769};
      case (8022):
        return new int[] {965, 787, 834};
      case (120663):
        return new int[] {946};
      case (120662):
        return new int[] {945};
      case (120665):
        return new int[] {948};
      case (8025):
        return new int[] {8017};
      case (120664):
        return new int[] {947};
      case (120667):
        return new int[] {950};
      case (8027):
        return new int[] {8019};
      case (120666):
        return new int[] {949};
      case (120669):
        return new int[] {952};
      case (8029):
        return new int[] {8021};
      case (120668):
        return new int[] {951};
      case (120671):
        return new int[] {954};
      case (8031):
        return new int[] {8023};
      case (120670):
        return new int[] {953};
      case (120673):
        return new int[] {956};
      case (120672):
        return new int[] {955};
      case (120675):
        return new int[] {958};
      case (120674):
        return new int[] {957};
      case (120677):
        return new int[] {960};
      case (120676):
        return new int[] {959};
      case (120679):
        return new int[] {952};
      case (120678):
        return new int[] {961};
      case (8040):
        return new int[] {8032};
      case (120681):
        return new int[] {964};
      case (8041):
        return new int[] {8033};
      case (120680):
        return new int[] {963};
      case (8042):
        return new int[] {8034};
      case (120683):
        return new int[] {966};
      case (8043):
        return new int[] {8035};
      case (120682):
        return new int[] {965};
      case (8044):
        return new int[] {8036};
      case (120685):
        return new int[] {968};
      case (8045):
        return new int[] {8037};
      case (120684):
        return new int[] {967};
      case (8046):
        return new int[] {8038};
      case (8047):
        return new int[] {8039};
      case (120686):
        return new int[] {969};
      case (8064):
        return new int[] {7936, 953};
      case (120705):
        return new int[] {963};
      case (8065):
        return new int[] {7937, 953};
      case (8066):
        return new int[] {7938, 953};
      case (8067):
        return new int[] {7939, 953};
      case (8068):
        return new int[] {7940, 953};
      case (8069):
        return new int[] {7941, 953};
      case (8070):
        return new int[] {7942, 953};
      case (8071):
        return new int[] {7943, 953};
      case (8072):
        return new int[] {7936, 953};
      case (8073):
        return new int[] {7937, 953};
      case (8074):
        return new int[] {7938, 953};
      case (8075):
        return new int[] {7939, 953};
      case (8076):
        return new int[] {7940, 953};
      case (8077):
        return new int[] {7941, 953};
      case (8078):
        return new int[] {7942, 953};
      case (8079):
        return new int[] {7943, 953};
      case (8080):
        return new int[] {7968, 953};
      case (120721):
        return new int[] {946};
      case (8081):
        return new int[] {7969, 953};
      case (120720):
        return new int[] {945};
      case (8082):
        return new int[] {7970, 953};
      case (120723):
        return new int[] {948};
      case (8083):
        return new int[] {7971, 953};
      case (120722):
        return new int[] {947};
      case (8084):
        return new int[] {7972, 953};
      case (120725):
        return new int[] {950};
      case (8085):
        return new int[] {7973, 953};
      case (120724):
        return new int[] {949};
      case (8086):
        return new int[] {7974, 953};
      case (120727):
        return new int[] {952};
      case (8087):
        return new int[] {7975, 953};
      case (120726):
        return new int[] {951};
      case (8088):
        return new int[] {7968, 953};
      case (120729):
        return new int[] {954};
      case (8089):
        return new int[] {7969, 953};
      case (120728):
        return new int[] {953};
      case (8090):
        return new int[] {7970, 953};
      case (120731):
        return new int[] {956};
      case (8091):
        return new int[] {7971, 953};
      case (120730):
        return new int[] {955};
      case (8092):
        return new int[] {7972, 953};
      case (120733):
        return new int[] {958};
      case (8093):
        return new int[] {7973, 953};
      case (120732):
        return new int[] {957};
      case (8094):
        return new int[] {7974, 953};
      case (120735):
        return new int[] {960};
      case (8095):
        return new int[] {7975, 953};
      case (120734):
        return new int[] {959};
      case (8096):
        return new int[] {8032, 953};
      case (120737):
        return new int[] {952};
      case (8097):
        return new int[] {8033, 953};
      case (120736):
        return new int[] {961};
      case (8098):
        return new int[] {8034, 953};
      case (120739):
        return new int[] {964};
      case (8099):
        return new int[] {8035, 953};
      case (120738):
        return new int[] {963};
      case (8100):
        return new int[] {8036, 953};
      case (120741):
        return new int[] {966};
      case (8101):
        return new int[] {8037, 953};
      case (120740):
        return new int[] {965};
      case (8102):
        return new int[] {8038, 953};
      case (120743):
        return new int[] {968};
      case (8103):
        return new int[] {8039, 953};
      case (120742):
        return new int[] {967};
      case (8104):
        return new int[] {8032, 953};
      case (8105):
        return new int[] {8033, 953};
      case (120744):
        return new int[] {969};
      case (8106):
        return new int[] {8034, 953};
      case (8107):
        return new int[] {8035, 953};
      case (8108):
        return new int[] {8036, 953};
      case (8109):
        return new int[] {8037, 953};
      case (8110):
        return new int[] {8038, 953};
      case (8111):
        return new int[] {8039, 953};
      case (8114):
        return new int[] {8048, 953};
      case (8115):
        return new int[] {945, 953};
      case (8116):
        return new int[] {940, 953};
      case (8118):
        return new int[] {945, 834};
      case (8119):
        return new int[] {945, 834, 953};
      case (8120):
        return new int[] {8112};
      case (8121):
        return new int[] {8113};
      case (8122):
        return new int[] {8048};
      case (120763):
        return new int[] {963};
      case (8123):
        return new int[] {8049};
      case (8124):
        return new int[] {945, 953};
      case (8126):
        return new int[] {953};
      case (8130):
        return new int[] {8052, 953};
      case (8131):
        return new int[] {951, 953};
      case (8132):
        return new int[] {942, 953};
      case (8134):
        return new int[] {951, 834};
      case (8135):
        return new int[] {951, 834, 953};
      case (8136):
        return new int[] {8050};
      case (8137):
        return new int[] {8051};
      case (8138):
        return new int[] {8052};
      case (8139):
        return new int[] {8053};
      case (8140):
        return new int[] {951, 953};
      case (8146):
        return new int[] {953, 776, 768};
      case (8147):
        return new int[] {953, 776, 769};
      case (8150):
        return new int[] {953, 834};
      case (8151):
        return new int[] {953, 776, 834};
      case (8152):
        return new int[] {8144};
      case (8153):
        return new int[] {8145};
      case (8154):
        return new int[] {8054};
      case (8155):
        return new int[] {8055};
      case (8162):
        return new int[] {965, 776, 768};
      case (8163):
        return new int[] {965, 776, 769};
      case (8164):
        return new int[] {961, 787};
      case (8166):
        return new int[] {965, 834};
      case (8167):
        return new int[] {965, 776, 834};
      case (8168):
        return new int[] {8160};
      case (8169):
        return new int[] {8161};
      case (8170):
        return new int[] {8058};
      case (8171):
        return new int[] {8059};
      case (8172):
        return new int[] {8165};
      case (8178):
        return new int[] {8060, 953};
      case (8179):
        return new int[] {969, 953};
      case (8180):
        return new int[] {974, 953};
      case (8182):
        return new int[] {969, 834};
      case (8183):
        return new int[] {969, 834, 953};
      case (8184):
        return new int[] {8056};
      case (8185):
        return new int[] {8057};
      case (8186):
        return new int[] {8060};
      case (8187):
        return new int[] {8061};
      case (8188):
        return new int[] {969, 953};
      default:
        return new int[] {codepoint};
    }
  }

  public static int[] mapUsedWithNoNormalization(int codepoint) {
    switch (codepoint) {
      case (65):
        return new int[] {97};
      case (66):
        return new int[] {98};
      case (67):
        return new int[] {99};
      case (68):
        return new int[] {100};
      case (69):
        return new int[] {101};
      case (70):
        return new int[] {102};
      case (71):
        return new int[] {103};
      case (72):
        return new int[] {104};
      case (73):
        return new int[] {105};
      case (74):
        return new int[] {106};
      case (75):
        return new int[] {107};
      case (76):
        return new int[] {108};
      case (77):
        return new int[] {109};
      case (78):
        return new int[] {110};
      case (79):
        return new int[] {111};
      case (80):
        return new int[] {112};
      case (81):
        return new int[] {113};
      case (82):
        return new int[] {114};
      case (83):
        return new int[] {115};
      case (84):
        return new int[] {116};
      case (85):
        return new int[] {117};
      case (86):
        return new int[] {118};
      case (87):
        return new int[] {119};
      case (88):
        return new int[] {120};
      case (89):
        return new int[] {121};
      case (90):
        return new int[] {122};
      case (181):
        return new int[] {956};
      case (192):
        return new int[] {224};
      case (193):
        return new int[] {225};
      case (194):
        return new int[] {226};
      case (195):
        return new int[] {227};
      case (196):
        return new int[] {228};
      case (197):
        return new int[] {229};
      case (198):
        return new int[] {230};
      case (199):
        return new int[] {231};
      case (200):
        return new int[] {232};
      case (201):
        return new int[] {233};
      case (202):
        return new int[] {234};
      case (203):
        return new int[] {235};
      case (204):
        return new int[] {236};
      case (205):
        return new int[] {237};
      case (206):
        return new int[] {238};
      case (207):
        return new int[] {239};
      case (208):
        return new int[] {240};
      case (209):
        return new int[] {241};
      case (210):
        return new int[] {242};
      case (211):
        return new int[] {243};
      case (212):
        return new int[] {244};
      case (213):
        return new int[] {245};
      case (214):
        return new int[] {246};
      case (216):
        return new int[] {248};
      case (217):
        return new int[] {249};
      case (218):
        return new int[] {250};
      case (219):
        return new int[] {251};
      case (220):
        return new int[] {252};
      case (221):
        return new int[] {253};
      case (222):
        return new int[] {254};
      case (223):
        return new int[] {115, 115};
      case (256):
        return new int[] {257};
      case (258):
        return new int[] {259};
      case (260):
        return new int[] {261};
      case (262):
        return new int[] {263};
      case (264):
        return new int[] {265};
      case (266):
        return new int[] {267};
      case (268):
        return new int[] {269};
      case (270):
        return new int[] {271};
      case (272):
        return new int[] {273};
      case (274):
        return new int[] {275};
      case (276):
        return new int[] {277};
      case (278):
        return new int[] {279};
      case (280):
        return new int[] {281};
      case (282):
        return new int[] {283};
      case (284):
        return new int[] {285};
      case (286):
        return new int[] {287};
      case (288):
        return new int[] {289};
      case (290):
        return new int[] {291};
      case (292):
        return new int[] {293};
      case (294):
        return new int[] {295};
      case (8486):
        return new int[] {969};
      case (296):
        return new int[] {297};
      case (298):
        return new int[] {299};
      case (8490):
        return new int[] {107};
      case (8491):
        return new int[] {229};
      case (300):
        return new int[] {301};
      case (302):
        return new int[] {303};
      case (304):
        return new int[] {105, 775};
      case (306):
        return new int[] {307};
      case (308):
        return new int[] {309};
      case (310):
        return new int[] {311};
      case (313):
        return new int[] {314};
      case (315):
        return new int[] {316};
      case (317):
        return new int[] {318};
      case (319):
        return new int[] {320};
      case (321):
        return new int[] {322};
      case (323):
        return new int[] {324};
      case (325):
        return new int[] {326};
      case (327):
        return new int[] {328};
      case (329):
        return new int[] {700, 110};
      case (330):
        return new int[] {331};
      case (332):
        return new int[] {333};
      case (334):
        return new int[] {335};
      case (336):
        return new int[] {337};
      case (338):
        return new int[] {339};
      case (340):
        return new int[] {341};
      case (342):
        return new int[] {343};
      case (344):
        return new int[] {345};
      case (346):
        return new int[] {347};
      case (348):
        return new int[] {349};
      case (350):
        return new int[] {351};
      case (352):
        return new int[] {353};
      case (8544):
        return new int[] {8560};
      case (8545):
        return new int[] {8561};
      case (354):
        return new int[] {355};
      case (8546):
        return new int[] {8562};
      case (8547):
        return new int[] {8563};
      case (356):
        return new int[] {357};
      case (8548):
        return new int[] {8564};
      case (8549):
        return new int[] {8565};
      case (358):
        return new int[] {359};
      case (8550):
        return new int[] {8566};
      case (8551):
        return new int[] {8567};
      case (360):
        return new int[] {361};
      case (8552):
        return new int[] {8568};
      case (8553):
        return new int[] {8569};
      case (362):
        return new int[] {363};
      case (8554):
        return new int[] {8570};
      case (8555):
        return new int[] {8571};
      case (364):
        return new int[] {365};
      case (8556):
        return new int[] {8572};
      case (8557):
        return new int[] {8573};
      case (366):
        return new int[] {367};
      case (8558):
        return new int[] {8574};
      case (8559):
        return new int[] {8575};
      case (368):
        return new int[] {369};
      case (370):
        return new int[] {371};
      case (372):
        return new int[] {373};
      case (374):
        return new int[] {375};
      case (376):
        return new int[] {255};
      case (377):
        return new int[] {378};
      case (379):
        return new int[] {380};
      case (381):
        return new int[] {382};
      case (383):
        return new int[] {115};
      case (385):
        return new int[] {595};
      case (386):
        return new int[] {387};
      case (388):
        return new int[] {389};
      case (390):
        return new int[] {596};
      case (391):
        return new int[] {392};
      case (393):
        return new int[] {598};
      case (394):
        return new int[] {599};
      case (395):
        return new int[] {396};
      case (398):
        return new int[] {477};
      case (399):
        return new int[] {601};
      case (400):
        return new int[] {603};
      case (401):
        return new int[] {402};
      case (403):
        return new int[] {608};
      case (404):
        return new int[] {611};
      case (406):
        return new int[] {617};
      case (407):
        return new int[] {616};
      case (408):
        return new int[] {409};
      case (412):
        return new int[] {623};
      case (413):
        return new int[] {626};
      case (415):
        return new int[] {629};
      case (416):
        return new int[] {417};
      case (418):
        return new int[] {419};
      case (420):
        return new int[] {421};
      case (422):
        return new int[] {640};
      case (423):
        return new int[] {424};
      case (425):
        return new int[] {643};
      case (428):
        return new int[] {429};
      case (430):
        return new int[] {648};
      case (431):
        return new int[] {432};
      case (433):
        return new int[] {650};
      case (434):
        return new int[] {651};
      case (435):
        return new int[] {436};
      case (437):
        return new int[] {438};
      case (439):
        return new int[] {658};
      case (440):
        return new int[] {441};
      case (444):
        return new int[] {445};
      case (452):
        return new int[] {454};
      case (453):
        return new int[] {454};
      case (455):
        return new int[] {457};
      case (456):
        return new int[] {457};
      case (458):
        return new int[] {460};
      case (459):
        return new int[] {460};
      case (461):
        return new int[] {462};
      case (463):
        return new int[] {464};
      case (465):
        return new int[] {466};
      case (467):
        return new int[] {468};
      case (469):
        return new int[] {470};
      case (471):
        return new int[] {472};
      case (473):
        return new int[] {474};
      case (475):
        return new int[] {476};
      case (478):
        return new int[] {479};
      case (480):
        return new int[] {481};
      case (482):
        return new int[] {483};
      case (484):
        return new int[] {485};
      case (486):
        return new int[] {487};
      case (488):
        return new int[] {489};
      case (490):
        return new int[] {491};
      case (492):
        return new int[] {493};
      case (494):
        return new int[] {495};
      case (496):
        return new int[] {106, 780};
      case (497):
        return new int[] {499};
      case (498):
        return new int[] {499};
      case (500):
        return new int[] {501};
      case (502):
        return new int[] {405};
      case (503):
        return new int[] {447};
      case (504):
        return new int[] {505};
      case (506):
        return new int[] {507};
      case (508):
        return new int[] {509};
      case (510):
        return new int[] {511};
      case (512):
        return new int[] {513};
      case (514):
        return new int[] {515};
      case (516):
        return new int[] {517};
      case (518):
        return new int[] {519};
      case (520):
        return new int[] {521};
      case (522):
        return new int[] {523};
      case (524):
        return new int[] {525};
      case (526):
        return new int[] {527};
      case (528):
        return new int[] {529};
      case (530):
        return new int[] {531};
      case (532):
        return new int[] {533};
      case (534):
        return new int[] {535};
      case (536):
        return new int[] {537};
      case (538):
        return new int[] {539};
      case (540):
        return new int[] {541};
      case (542):
        return new int[] {543};
      case (544):
        return new int[] {414};
      case (546):
        return new int[] {547};
      case (548):
        return new int[] {549};
      case (550):
        return new int[] {551};
      case (552):
        return new int[] {553};
      case (554):
        return new int[] {555};
      case (556):
        return new int[] {557};
      case (558):
        return new int[] {559};
      case (560):
        return new int[] {561};
      case (562):
        return new int[] {563};
      case (64256):
        return new int[] {102, 102};
      case (64257):
        return new int[] {102, 105};
      case (64258):
        return new int[] {102, 108};
      case (64259):
        return new int[] {102, 102, 105};
      case (64260):
        return new int[] {102, 102, 108};
      case (64261):
        return new int[] {115, 116};
      case (64262):
        return new int[] {115, 116};
      case (64275):
        return new int[] {1396, 1398};
      case (64276):
        return new int[] {1396, 1381};
      case (64277):
        return new int[] {1396, 1387};
      case (64278):
        return new int[] {1406, 1398};
      case (64279):
        return new int[] {1396, 1389};
      case (837):
        return new int[] {953};
      case (902):
        return new int[] {940};
      case (904):
        return new int[] {941};
      case (905):
        return new int[] {942};
      case (906):
        return new int[] {943};
      case (908):
        return new int[] {972};
      case (910):
        return new int[] {973};
      case (911):
        return new int[] {974};
      case (912):
        return new int[] {953, 776, 769};
      case (913):
        return new int[] {945};
      case (914):
        return new int[] {946};
      case (915):
        return new int[] {947};
      case (916):
        return new int[] {948};
      case (917):
        return new int[] {949};
      case (918):
        return new int[] {950};
      case (919):
        return new int[] {951};
      case (920):
        return new int[] {952};
      case (921):
        return new int[] {953};
      case (922):
        return new int[] {954};
      case (923):
        return new int[] {955};
      case (924):
        return new int[] {956};
      case (925):
        return new int[] {957};
      case (926):
        return new int[] {958};
      case (927):
        return new int[] {959};
      case (928):
        return new int[] {960};
      case (929):
        return new int[] {961};
      case (931):
        return new int[] {963};
      case (932):
        return new int[] {964};
      case (933):
        return new int[] {965};
      case (934):
        return new int[] {966};
      case (935):
        return new int[] {967};
      case (936):
        return new int[] {968};
      case (937):
        return new int[] {969};
      case (938):
        return new int[] {970};
      case (939):
        return new int[] {971};
      case (944):
        return new int[] {965, 776, 769};
      case (962):
        return new int[] {963};
      case (976):
        return new int[] {946};
      case (977):
        return new int[] {952};
      case (981):
        return new int[] {966};
      case (982):
        return new int[] {960};
      case (984):
        return new int[] {985};
      case (986):
        return new int[] {987};
      case (988):
        return new int[] {989};
      case (990):
        return new int[] {991};
      case (992):
        return new int[] {993};
      case (994):
        return new int[] {995};
      case (996):
        return new int[] {997};
      case (998):
        return new int[] {999};
      case (1000):
        return new int[] {1001};
      case (1002):
        return new int[] {1003};
      case (1004):
        return new int[] {1005};
      case (1006):
        return new int[] {1007};
      case (1008):
        return new int[] {954};
      case (1009):
        return new int[] {961};
      case (1010):
        return new int[] {963};
      case (1012):
        return new int[] {952};
      case (1013):
        return new int[] {949};
      case (1024):
        return new int[] {1104};
      case (66561):
        return new int[] {66601};
      case (1025):
        return new int[] {1105};
      case (66560):
        return new int[] {66600};
      case (1026):
        return new int[] {1106};
      case (66563):
        return new int[] {66603};
      case (1027):
        return new int[] {1107};
      case (66562):
        return new int[] {66602};
      case (1028):
        return new int[] {1108};
      case (66565):
        return new int[] {66605};
      case (1029):
        return new int[] {1109};
      case (66564):
        return new int[] {66604};
      case (1030):
        return new int[] {1110};
      case (66567):
        return new int[] {66607};
      case (1031):
        return new int[] {1111};
      case (66566):
        return new int[] {66606};
      case (1032):
        return new int[] {1112};
      case (66569):
        return new int[] {66609};
      case (1033):
        return new int[] {1113};
      case (66568):
        return new int[] {66608};
      case (1034):
        return new int[] {1114};
      case (66571):
        return new int[] {66611};
      case (1035):
        return new int[] {1115};
      case (66570):
        return new int[] {66610};
      case (1036):
        return new int[] {1116};
      case (66573):
        return new int[] {66613};
      case (1037):
        return new int[] {1117};
      case (66572):
        return new int[] {66612};
      case (1038):
        return new int[] {1118};
      case (66575):
        return new int[] {66615};
      case (1039):
        return new int[] {1119};
      case (66574):
        return new int[] {66614};
      case (1040):
        return new int[] {1072};
      case (66577):
        return new int[] {66617};
      case (1041):
        return new int[] {1073};
      case (66576):
        return new int[] {66616};
      case (1042):
        return new int[] {1074};
      case (66579):
        return new int[] {66619};
      case (1043):
        return new int[] {1075};
      case (66578):
        return new int[] {66618};
      case (1044):
        return new int[] {1076};
      case (66581):
        return new int[] {66621};
      case (1045):
        return new int[] {1077};
      case (66580):
        return new int[] {66620};
      case (1046):
        return new int[] {1078};
      case (66583):
        return new int[] {66623};
      case (1047):
        return new int[] {1079};
      case (66582):
        return new int[] {66622};
      case (1048):
        return new int[] {1080};
      case (66585):
        return new int[] {66625};
      case (1049):
        return new int[] {1081};
      case (66584):
        return new int[] {66624};
      case (1050):
        return new int[] {1082};
      case (66587):
        return new int[] {66627};
      case (1051):
        return new int[] {1083};
      case (66586):
        return new int[] {66626};
      case (1052):
        return new int[] {1084};
      case (66589):
        return new int[] {66629};
      case (1053):
        return new int[] {1085};
      case (66588):
        return new int[] {66628};
      case (1054):
        return new int[] {1086};
      case (66591):
        return new int[] {66631};
      case (1055):
        return new int[] {1087};
      case (66590):
        return new int[] {66630};
      case (1056):
        return new int[] {1088};
      case (66593):
        return new int[] {66633};
      case (1057):
        return new int[] {1089};
      case (66592):
        return new int[] {66632};
      case (1058):
        return new int[] {1090};
      case (66595):
        return new int[] {66635};
      case (1059):
        return new int[] {1091};
      case (66594):
        return new int[] {66634};
      case (1060):
        return new int[] {1092};
      case (66597):
        return new int[] {66637};
      case (1061):
        return new int[] {1093};
      case (66596):
        return new int[] {66636};
      case (1062):
        return new int[] {1094};
      case (1063):
        return new int[] {1095};
      case (1064):
        return new int[] {1096};
      case (1065):
        return new int[] {1097};
      case (1066):
        return new int[] {1098};
      case (1067):
        return new int[] {1099};
      case (1068):
        return new int[] {1100};
      case (1069):
        return new int[] {1101};
      case (1070):
        return new int[] {1102};
      case (1071):
        return new int[] {1103};
      case (1120):
        return new int[] {1121};
      case (1122):
        return new int[] {1123};
      case (1124):
        return new int[] {1125};
      case (1126):
        return new int[] {1127};
      case (1128):
        return new int[] {1129};
      case (1130):
        return new int[] {1131};
      case (1132):
        return new int[] {1133};
      case (1134):
        return new int[] {1135};
      case (1136):
        return new int[] {1137};
      case (1138):
        return new int[] {1139};
      case (1140):
        return new int[] {1141};
      case (1142):
        return new int[] {1143};
      case (1144):
        return new int[] {1145};
      case (1146):
        return new int[] {1147};
      case (1148):
        return new int[] {1149};
      case (1150):
        return new int[] {1151};
      case (1152):
        return new int[] {1153};
      case (1162):
        return new int[] {1163};
      case (1164):
        return new int[] {1165};
      case (1166):
        return new int[] {1167};
      case (1168):
        return new int[] {1169};
      case (1170):
        return new int[] {1171};
      case (1172):
        return new int[] {1173};
      case (1174):
        return new int[] {1175};
      case (1176):
        return new int[] {1177};
      case (1178):
        return new int[] {1179};
      case (1180):
        return new int[] {1181};
      case (1182):
        return new int[] {1183};
      case (1184):
        return new int[] {1185};
      case (1186):
        return new int[] {1187};
      case (1188):
        return new int[] {1189};
      case (1190):
        return new int[] {1191};
      case (1192):
        return new int[] {1193};
      case (1194):
        return new int[] {1195};
      case (1196):
        return new int[] {1197};
      case (1198):
        return new int[] {1199};
      case (1200):
        return new int[] {1201};
      case (1202):
        return new int[] {1203};
      case (1204):
        return new int[] {1205};
      case (1206):
        return new int[] {1207};
      case (9398):
        return new int[] {9424};
      case (9399):
        return new int[] {9425};
      case (1208):
        return new int[] {1209};
      case (9400):
        return new int[] {9426};
      case (9401):
        return new int[] {9427};
      case (1210):
        return new int[] {1211};
      case (9402):
        return new int[] {9428};
      case (9403):
        return new int[] {9429};
      case (1212):
        return new int[] {1213};
      case (9404):
        return new int[] {9430};
      case (9405):
        return new int[] {9431};
      case (1214):
        return new int[] {1215};
      case (9406):
        return new int[] {9432};
      case (9407):
        return new int[] {9433};
      case (9408):
        return new int[] {9434};
      case (1217):
        return new int[] {1218};
      case (9409):
        return new int[] {9435};
      case (9410):
        return new int[] {9436};
      case (1219):
        return new int[] {1220};
      case (9411):
        return new int[] {9437};
      case (9412):
        return new int[] {9438};
      case (1221):
        return new int[] {1222};
      case (9413):
        return new int[] {9439};
      case (9414):
        return new int[] {9440};
      case (1223):
        return new int[] {1224};
      case (9415):
        return new int[] {9441};
      case (9416):
        return new int[] {9442};
      case (1225):
        return new int[] {1226};
      case (9417):
        return new int[] {9443};
      case (9418):
        return new int[] {9444};
      case (1227):
        return new int[] {1228};
      case (9419):
        return new int[] {9445};
      case (9420):
        return new int[] {9446};
      case (1229):
        return new int[] {1230};
      case (9421):
        return new int[] {9447};
      case (9422):
        return new int[] {9448};
      case (9423):
        return new int[] {9449};
      case (1232):
        return new int[] {1233};
      case (1234):
        return new int[] {1235};
      case (1236):
        return new int[] {1237};
      case (1238):
        return new int[] {1239};
      case (1240):
        return new int[] {1241};
      case (1242):
        return new int[] {1243};
      case (1244):
        return new int[] {1245};
      case (1246):
        return new int[] {1247};
      case (1248):
        return new int[] {1249};
      case (1250):
        return new int[] {1251};
      case (1252):
        return new int[] {1253};
      case (1254):
        return new int[] {1255};
      case (1256):
        return new int[] {1257};
      case (1258):
        return new int[] {1259};
      case (1260):
        return new int[] {1261};
      case (1262):
        return new int[] {1263};
      case (1264):
        return new int[] {1265};
      case (1266):
        return new int[] {1267};
      case (1268):
        return new int[] {1269};
      case (1272):
        return new int[] {1273};
      case (1280):
        return new int[] {1281};
      case (1282):
        return new int[] {1283};
      case (1284):
        return new int[] {1285};
      case (1286):
        return new int[] {1287};
      case (1288):
        return new int[] {1289};
      case (1290):
        return new int[] {1291};
      case (1292):
        return new int[] {1293};
      case (1294):
        return new int[] {1295};
      case (1329):
        return new int[] {1377};
      case (1330):
        return new int[] {1378};
      case (1331):
        return new int[] {1379};
      case (1332):
        return new int[] {1380};
      case (1333):
        return new int[] {1381};
      case (1334):
        return new int[] {1382};
      case (1335):
        return new int[] {1383};
      case (1336):
        return new int[] {1384};
      case (1337):
        return new int[] {1385};
      case (1338):
        return new int[] {1386};
      case (1339):
        return new int[] {1387};
      case (1340):
        return new int[] {1388};
      case (1341):
        return new int[] {1389};
      case (1342):
        return new int[] {1390};
      case (1343):
        return new int[] {1391};
      case (1344):
        return new int[] {1392};
      case (1345):
        return new int[] {1393};
      case (1346):
        return new int[] {1394};
      case (1347):
        return new int[] {1395};
      case (1348):
        return new int[] {1396};
      case (1349):
        return new int[] {1397};
      case (1350):
        return new int[] {1398};
      case (1351):
        return new int[] {1399};
      case (1352):
        return new int[] {1400};
      case (1353):
        return new int[] {1401};
      case (1354):
        return new int[] {1402};
      case (1355):
        return new int[] {1403};
      case (1356):
        return new int[] {1404};
      case (1357):
        return new int[] {1405};
      case (1358):
        return new int[] {1406};
      case (1359):
        return new int[] {1407};
      case (1360):
        return new int[] {1408};
      case (1361):
        return new int[] {1409};
      case (1362):
        return new int[] {1410};
      case (1363):
        return new int[] {1411};
      case (1364):
        return new int[] {1412};
      case (1365):
        return new int[] {1413};
      case (1366):
        return new int[] {1414};
      case (1415):
        return new int[] {1381, 1410};
      case (7680):
        return new int[] {7681};
      case (7682):
        return new int[] {7683};
      case (7684):
        return new int[] {7685};
      case (7686):
        return new int[] {7687};
      case (7688):
        return new int[] {7689};
      case (7690):
        return new int[] {7691};
      case (7692):
        return new int[] {7693};
      case (7694):
        return new int[] {7695};
      case (7696):
        return new int[] {7697};
      case (7698):
        return new int[] {7699};
      case (7700):
        return new int[] {7701};
      case (7702):
        return new int[] {7703};
      case (7704):
        return new int[] {7705};
      case (7706):
        return new int[] {7707};
      case (7708):
        return new int[] {7709};
      case (7710):
        return new int[] {7711};
      case (7712):
        return new int[] {7713};
      case (7714):
        return new int[] {7715};
      case (7716):
        return new int[] {7717};
      case (7718):
        return new int[] {7719};
      case (7720):
        return new int[] {7721};
      case (7722):
        return new int[] {7723};
      case (7724):
        return new int[] {7725};
      case (7726):
        return new int[] {7727};
      case (7728):
        return new int[] {7729};
      case (7730):
        return new int[] {7731};
      case (7732):
        return new int[] {7733};
      case (7734):
        return new int[] {7735};
      case (7736):
        return new int[] {7737};
      case (7738):
        return new int[] {7739};
      case (7740):
        return new int[] {7741};
      case (7742):
        return new int[] {7743};
      case (7744):
        return new int[] {7745};
      case (7746):
        return new int[] {7747};
      case (7748):
        return new int[] {7749};
      case (7750):
        return new int[] {7751};
      case (7752):
        return new int[] {7753};
      case (7754):
        return new int[] {7755};
      case (7756):
        return new int[] {7757};
      case (7758):
        return new int[] {7759};
      case (7760):
        return new int[] {7761};
      case (7762):
        return new int[] {7763};
      case (7764):
        return new int[] {7765};
      case (7766):
        return new int[] {7767};
      case (7768):
        return new int[] {7769};
      case (7770):
        return new int[] {7771};
      case (7772):
        return new int[] {7773};
      case (7774):
        return new int[] {7775};
      case (7776):
        return new int[] {7777};
      case (7778):
        return new int[] {7779};
      case (7780):
        return new int[] {7781};
      case (7782):
        return new int[] {7783};
      case (7784):
        return new int[] {7785};
      case (7786):
        return new int[] {7787};
      case (7788):
        return new int[] {7789};
      case (7790):
        return new int[] {7791};
      case (7792):
        return new int[] {7793};
      case (7794):
        return new int[] {7795};
      case (7796):
        return new int[] {7797};
      case (7798):
        return new int[] {7799};
      case (7800):
        return new int[] {7801};
      case (7802):
        return new int[] {7803};
      case (7804):
        return new int[] {7805};
      case (7806):
        return new int[] {7807};
      case (7808):
        return new int[] {7809};
      case (7810):
        return new int[] {7811};
      case (7812):
        return new int[] {7813};
      case (7814):
        return new int[] {7815};
      case (7816):
        return new int[] {7817};
      case (7818):
        return new int[] {7819};
      case (7820):
        return new int[] {7821};
      case (7822):
        return new int[] {7823};
      case (7824):
        return new int[] {7825};
      case (7826):
        return new int[] {7827};
      case (7828):
        return new int[] {7829};
      case (7830):
        return new int[] {104, 817};
      case (7831):
        return new int[] {116, 776};
      case (7832):
        return new int[] {119, 778};
      case (7833):
        return new int[] {121, 778};
      case (7834):
        return new int[] {97, 702};
      case (7835):
        return new int[] {7777};
      case (7840):
        return new int[] {7841};
      case (7842):
        return new int[] {7843};
      case (7844):
        return new int[] {7845};
      case (7846):
        return new int[] {7847};
      case (7848):
        return new int[] {7849};
      case (7850):
        return new int[] {7851};
      case (7852):
        return new int[] {7853};
      case (7854):
        return new int[] {7855};
      case (7856):
        return new int[] {7857};
      case (7858):
        return new int[] {7859};
      case (7860):
        return new int[] {7861};
      case (7862):
        return new int[] {7863};
      case (7864):
        return new int[] {7865};
      case (7866):
        return new int[] {7867};
      case (7868):
        return new int[] {7869};
      case (7870):
        return new int[] {7871};
      case (7872):
        return new int[] {7873};
      case (7874):
        return new int[] {7875};
      case (7876):
        return new int[] {7877};
      case (7878):
        return new int[] {7879};
      case (7880):
        return new int[] {7881};
      case (7882):
        return new int[] {7883};
      case (7884):
        return new int[] {7885};
      case (7886):
        return new int[] {7887};
      case (7888):
        return new int[] {7889};
      case (7890):
        return new int[] {7891};
      case (7892):
        return new int[] {7893};
      case (7894):
        return new int[] {7895};
      case (7896):
        return new int[] {7897};
      case (7898):
        return new int[] {7899};
      case (7900):
        return new int[] {7901};
      case (7902):
        return new int[] {7903};
      case (7904):
        return new int[] {7905};
      case (7906):
        return new int[] {7907};
      case (7908):
        return new int[] {7909};
      case (7910):
        return new int[] {7911};
      case (7912):
        return new int[] {7913};
      case (7914):
        return new int[] {7915};
      case (7916):
        return new int[] {7917};
      case (7918):
        return new int[] {7919};
      case (7920):
        return new int[] {7921};
      case (7922):
        return new int[] {7923};
      case (7924):
        return new int[] {7925};
      case (7926):
        return new int[] {7927};
      case (7928):
        return new int[] {7929};
      case (7944):
        return new int[] {7936};
      case (7945):
        return new int[] {7937};
      case (7946):
        return new int[] {7938};
      case (7947):
        return new int[] {7939};
      case (7948):
        return new int[] {7940};
      case (7949):
        return new int[] {7941};
      case (7950):
        return new int[] {7942};
      case (7951):
        return new int[] {7943};
      case (7960):
        return new int[] {7952};
      case (7961):
        return new int[] {7953};
      case (7962):
        return new int[] {7954};
      case (7963):
        return new int[] {7955};
      case (7964):
        return new int[] {7956};
      case (7965):
        return new int[] {7957};
      case (65313):
        return new int[] {65345};
      case (65314):
        return new int[] {65346};
      case (65315):
        return new int[] {65347};
      case (65316):
        return new int[] {65348};
      case (65317):
        return new int[] {65349};
      case (65318):
        return new int[] {65350};
      case (65319):
        return new int[] {65351};
      case (7976):
        return new int[] {7968};
      case (65320):
        return new int[] {65352};
      case (7977):
        return new int[] {7969};
      case (65321):
        return new int[] {65353};
      case (7978):
        return new int[] {7970};
      case (65322):
        return new int[] {65354};
      case (7979):
        return new int[] {7971};
      case (65323):
        return new int[] {65355};
      case (7980):
        return new int[] {7972};
      case (65324):
        return new int[] {65356};
      case (7981):
        return new int[] {7973};
      case (65325):
        return new int[] {65357};
      case (7982):
        return new int[] {7974};
      case (65326):
        return new int[] {65358};
      case (7983):
        return new int[] {7975};
      case (65327):
        return new int[] {65359};
      case (65328):
        return new int[] {65360};
      case (65329):
        return new int[] {65361};
      case (65330):
        return new int[] {65362};
      case (65331):
        return new int[] {65363};
      case (65332):
        return new int[] {65364};
      case (65333):
        return new int[] {65365};
      case (65334):
        return new int[] {65366};
      case (65335):
        return new int[] {65367};
      case (7992):
        return new int[] {7984};
      case (65336):
        return new int[] {65368};
      case (7993):
        return new int[] {7985};
      case (65337):
        return new int[] {65369};
      case (7994):
        return new int[] {7986};
      case (65338):
        return new int[] {65370};
      case (7995):
        return new int[] {7987};
      case (7996):
        return new int[] {7988};
      case (7997):
        return new int[] {7989};
      case (7998):
        return new int[] {7990};
      case (7999):
        return new int[] {7991};
      case (8008):
        return new int[] {8000};
      case (8009):
        return new int[] {8001};
      case (8010):
        return new int[] {8002};
      case (8011):
        return new int[] {8003};
      case (8012):
        return new int[] {8004};
      case (8013):
        return new int[] {8005};
      case (8016):
        return new int[] {965, 787};
      case (8018):
        return new int[] {965, 787, 768};
      case (8020):
        return new int[] {965, 787, 769};
      case (8022):
        return new int[] {965, 787, 834};
      case (8025):
        return new int[] {8017};
      case (8027):
        return new int[] {8019};
      case (8029):
        return new int[] {8021};
      case (8031):
        return new int[] {8023};
      case (8040):
        return new int[] {8032};
      case (8041):
        return new int[] {8033};
      case (8042):
        return new int[] {8034};
      case (8043):
        return new int[] {8035};
      case (8044):
        return new int[] {8036};
      case (8045):
        return new int[] {8037};
      case (8046):
        return new int[] {8038};
      case (8047):
        return new int[] {8039};
      case (8064):
        return new int[] {7936, 953};
      case (8065):
        return new int[] {7937, 953};
      case (8066):
        return new int[] {7938, 953};
      case (8067):
        return new int[] {7939, 953};
      case (8068):
        return new int[] {7940, 953};
      case (8069):
        return new int[] {7941, 953};
      case (8070):
        return new int[] {7942, 953};
      case (8071):
        return new int[] {7943, 953};
      case (8072):
        return new int[] {7936, 953};
      case (8073):
        return new int[] {7937, 953};
      case (8074):
        return new int[] {7938, 953};
      case (8075):
        return new int[] {7939, 953};
      case (8076):
        return new int[] {7940, 953};
      case (8077):
        return new int[] {7941, 953};
      case (8078):
        return new int[] {7942, 953};
      case (8079):
        return new int[] {7943, 953};
      case (8080):
        return new int[] {7968, 953};
      case (8081):
        return new int[] {7969, 953};
      case (8082):
        return new int[] {7970, 953};
      case (8083):
        return new int[] {7971, 953};
      case (8084):
        return new int[] {7972, 953};
      case (8085):
        return new int[] {7973, 953};
      case (8086):
        return new int[] {7974, 953};
      case (8087):
        return new int[] {7975, 953};
      case (8088):
        return new int[] {7968, 953};
      case (8089):
        return new int[] {7969, 953};
      case (8090):
        return new int[] {7970, 953};
      case (8091):
        return new int[] {7971, 953};
      case (8092):
        return new int[] {7972, 953};
      case (8093):
        return new int[] {7973, 953};
      case (8094):
        return new int[] {7974, 953};
      case (8095):
        return new int[] {7975, 953};
      case (8096):
        return new int[] {8032, 953};
      case (8097):
        return new int[] {8033, 953};
      case (8098):
        return new int[] {8034, 953};
      case (8099):
        return new int[] {8035, 953};
      case (8100):
        return new int[] {8036, 953};
      case (8101):
        return new int[] {8037, 953};
      case (8102):
        return new int[] {8038, 953};
      case (8103):
        return new int[] {8039, 953};
      case (8104):
        return new int[] {8032, 953};
      case (8105):
        return new int[] {8033, 953};
      case (8106):
        return new int[] {8034, 953};
      case (8107):
        return new int[] {8035, 953};
      case (8108):
        return new int[] {8036, 953};
      case (8109):
        return new int[] {8037, 953};
      case (8110):
        return new int[] {8038, 953};
      case (8111):
        return new int[] {8039, 953};
      case (8114):
        return new int[] {8048, 953};
      case (8115):
        return new int[] {945, 953};
      case (8116):
        return new int[] {940, 953};
      case (8118):
        return new int[] {945, 834};
      case (8119):
        return new int[] {945, 834, 953};
      case (8120):
        return new int[] {8112};
      case (8121):
        return new int[] {8113};
      case (8122):
        return new int[] {8048};
      case (8123):
        return new int[] {8049};
      case (8124):
        return new int[] {945, 953};
      case (8126):
        return new int[] {953};
      case (8130):
        return new int[] {8052, 953};
      case (8131):
        return new int[] {951, 953};
      case (8132):
        return new int[] {942, 953};
      case (8134):
        return new int[] {951, 834};
      case (8135):
        return new int[] {951, 834, 953};
      case (8136):
        return new int[] {8050};
      case (8137):
        return new int[] {8051};
      case (8138):
        return new int[] {8052};
      case (8139):
        return new int[] {8053};
      case (8140):
        return new int[] {951, 953};
      case (8146):
        return new int[] {953, 776, 768};
      case (8147):
        return new int[] {953, 776, 769};
      case (8150):
        return new int[] {953, 834};
      case (8151):
        return new int[] {953, 776, 834};
      case (8152):
        return new int[] {8144};
      case (8153):
        return new int[] {8145};
      case (8154):
        return new int[] {8054};
      case (8155):
        return new int[] {8055};
      case (8162):
        return new int[] {965, 776, 768};
      case (8163):
        return new int[] {965, 776, 769};
      case (8164):
        return new int[] {961, 787};
      case (8166):
        return new int[] {965, 834};
      case (8167):
        return new int[] {965, 776, 834};
      case (8168):
        return new int[] {8160};
      case (8169):
        return new int[] {8161};
      case (8170):
        return new int[] {8058};
      case (8171):
        return new int[] {8059};
      case (8172):
        return new int[] {8165};
      case (8178):
        return new int[] {8060, 953};
      case (8179):
        return new int[] {969, 953};
      case (8180):
        return new int[] {974, 953};
      case (8182):
        return new int[] {969, 834};
      case (8183):
        return new int[] {969, 834, 953};
      case (8184):
        return new int[] {8056};
      case (8185):
        return new int[] {8057};
      case (8186):
        return new int[] {8060};
      case (8187):
        return new int[] {8061};
      case (8188):
        return new int[] {969, 953};
      default:
        return new int[] {codepoint};
    }
  }

  public static boolean prohibitionAsciiSpace(int codepoint) {
    return
        codepoint == 32
        ;
  }

  public static boolean prohibitionNonAsciiSpace(int codepoint) {
    return
        codepoint == 160 ||
        codepoint == 5760 ||
        codepoint == 8192 ||
        codepoint == 8193 ||
        codepoint == 8194 ||
        codepoint == 8195 ||
        codepoint == 8196 ||
        codepoint == 8197 ||
        codepoint == 8198 ||
        codepoint == 8199 ||
        codepoint == 8200 ||
        codepoint == 8201 ||
        codepoint == 8202 ||
        codepoint == 8203 ||
        codepoint == 8239 ||
        codepoint == 8287 ||
        codepoint == 12288
        ;
  }

  public static boolean prohibitionAsciiControl(int codepoint) {
    return
        codepoint >= 0 && codepoint <= 31 ||
        codepoint == 127
        ;
  }

  public static boolean prohibitionNonAsciiControl(int codepoint) {
    return
        codepoint >= 128 && codepoint <= 159 ||
        codepoint == 1757 ||
        codepoint == 1807 ||
        codepoint == 6158 ||
        codepoint == 8204 ||
        codepoint == 8205 ||
        codepoint == 8232 ||
        codepoint == 8233 ||
        codepoint == 8288 ||
        codepoint == 8289 ||
        codepoint == 8290 ||
        codepoint == 8291 ||
        codepoint >= 8298 && codepoint <= 8303 ||
        codepoint == 65279 ||
        codepoint >= 65529 && codepoint <= 65532 ||
        codepoint >= 119155 && codepoint <= 119162
        ;
  }

  public static boolean prohibitionPrivateUse(int codepoint) {
    return
        codepoint >= 57344 && codepoint <= 63743 ||
        codepoint >= 983040 && codepoint <= 1048573 ||
        codepoint >= 1048576 && codepoint <= 1114109
        ;
  }

  public static boolean prohibitionNonCharacterCodePoints(int codepoint) {
    return
        codepoint >= 64976 && codepoint <= 65007 ||
        codepoint >= 65534 && codepoint <= 65535 ||
        codepoint >= 131070 && codepoint <= 131071 ||
        codepoint >= 196606 && codepoint <= 196607 ||
        codepoint >= 262142 && codepoint <= 262143 ||
        codepoint >= 327678 && codepoint <= 327679 ||
        codepoint >= 393214 && codepoint <= 393215 ||
        codepoint >= 458750 && codepoint <= 458751 ||
        codepoint >= 524286 && codepoint <= 524287 ||
        codepoint >= 589822 && codepoint <= 589823 ||
        codepoint >= 655358 && codepoint <= 655359 ||
        codepoint >= 720894 && codepoint <= 720895 ||
        codepoint >= 786430 && codepoint <= 786431 ||
        codepoint >= 851966 && codepoint <= 851967 ||
        codepoint >= 917502 && codepoint <= 917503 ||
        codepoint >= 983038 && codepoint <= 983039 ||
        codepoint >= 1048574 && codepoint <= 1048575 ||
        codepoint >= 1114110 && codepoint <= 1114111
        ;
  }

  public static boolean prohibitionSurrogateCodes(int codepoint) {
    return
        codepoint >= 55296 && codepoint <= 57343
        ;
  }

  public static boolean prohibitionInappropriatePlainText(int codepoint) {
    return
        codepoint == 65529 ||
        codepoint == 65530 ||
        codepoint == 65531 ||
        codepoint == 65532 ||
        codepoint == 65533
        ;
  }

  public static boolean prohibitionInappropriateCanonicalRepresentation(int codepoint) {
    return
        codepoint >= 12272 && codepoint <= 12283
        ;
  }

  public static boolean prohibitionChangeDisplayProperties(int codepoint) {
    return
        codepoint == 832 ||
        codepoint == 833 ||
        codepoint == 8206 ||
        codepoint == 8207 ||
        codepoint == 8234 ||
        codepoint == 8235 ||
        codepoint == 8236 ||
        codepoint == 8237 ||
        codepoint == 8238 ||
        codepoint == 8298 ||
        codepoint == 8299 ||
        codepoint == 8300 ||
        codepoint == 8301 ||
        codepoint == 8302 ||
        codepoint == 8303
        ;
  }

  public static boolean prohibitionTaggingCharacters(int codepoint) {
    return
        codepoint == 917505 ||
        codepoint >= 917536 && codepoint <= 917631
        ;
  }

  public static boolean bidirectionalPropertyRorAL(int codepoint) {
    return
        codepoint == 1470 ||
        codepoint == 1472 ||
        codepoint == 1475 ||
        codepoint >= 1488 && codepoint <= 1514 ||
        codepoint >= 1520 && codepoint <= 1524 ||
        codepoint == 1563 ||
        codepoint == 1567 ||
        codepoint >= 1569 && codepoint <= 1594 ||
        codepoint >= 1600 && codepoint <= 1610 ||
        codepoint >= 1645 && codepoint <= 1647 ||
        codepoint >= 1649 && codepoint <= 1749 ||
        codepoint == 1757 ||
        codepoint >= 1765 && codepoint <= 1766 ||
        codepoint >= 1786 && codepoint <= 1790 ||
        codepoint >= 1792 && codepoint <= 1805 ||
        codepoint == 1808 ||
        codepoint >= 1810 && codepoint <= 1836 ||
        codepoint >= 1920 && codepoint <= 1957 ||
        codepoint == 1969 ||
        codepoint == 8207 ||
        codepoint == 64285 ||
        codepoint >= 64287 && codepoint <= 64296 ||
        codepoint >= 64298 && codepoint <= 64310 ||
        codepoint >= 64312 && codepoint <= 64316 ||
        codepoint == 64318 ||
        codepoint >= 64320 && codepoint <= 64321 ||
        codepoint >= 64323 && codepoint <= 64324 ||
        codepoint >= 64326 && codepoint <= 64433 ||
        codepoint >= 64467 && codepoint <= 64829 ||
        codepoint >= 64848 && codepoint <= 64911 ||
        codepoint >= 64914 && codepoint <= 64967 ||
        codepoint >= 65008 && codepoint <= 65020 ||
        codepoint >= 65136 && codepoint <= 65140 ||
        codepoint >= 65142 && codepoint <= 65276
        ;
  }

  public static boolean bidirectionalPropertyL(int codepoint) {
    return
        codepoint >= 65 && codepoint <= 90 ||
        codepoint >= 97 && codepoint <= 122 ||
        codepoint == 170 ||
        codepoint == 181 ||
        codepoint == 186 ||
        codepoint >= 192 && codepoint <= 214 ||
        codepoint >= 216 && codepoint <= 246 ||
        codepoint >= 248 && codepoint <= 544 ||
        codepoint >= 546 && codepoint <= 563 ||
        codepoint >= 592 && codepoint <= 685 ||
        codepoint >= 688 && codepoint <= 696 ||
        codepoint >= 699 && codepoint <= 705 ||
        codepoint >= 720 && codepoint <= 721 ||
        codepoint >= 736 && codepoint <= 740 ||
        codepoint == 750 ||
        codepoint == 890 ||
        codepoint == 902 ||
        codepoint >= 904 && codepoint <= 906 ||
        codepoint == 908 ||
        codepoint >= 910 && codepoint <= 929 ||
        codepoint >= 931 && codepoint <= 974 ||
        codepoint >= 976 && codepoint <= 1013 ||
        codepoint >= 1024 && codepoint <= 1154 ||
        codepoint >= 1162 && codepoint <= 1230 ||
        codepoint >= 1232 && codepoint <= 1269 ||
        codepoint >= 1272 && codepoint <= 1273 ||
        codepoint >= 1280 && codepoint <= 1295 ||
        codepoint >= 1329 && codepoint <= 1366 ||
        codepoint >= 1369 && codepoint <= 1375 ||
        codepoint >= 1377 && codepoint <= 1415 ||
        codepoint == 1417 ||
        codepoint == 2307 ||
        codepoint >= 2309 && codepoint <= 2361 ||
        codepoint >= 2365 && codepoint <= 2368 ||
        codepoint >= 2377 && codepoint <= 2380 ||
        codepoint == 2384 ||
        codepoint >= 2392 && codepoint <= 2401 ||
        codepoint >= 2404 && codepoint <= 2416 ||
        codepoint >= 2434 && codepoint <= 2435 ||
        codepoint >= 2437 && codepoint <= 2444 ||
        codepoint >= 2447 && codepoint <= 2448 ||
        codepoint >= 2451 && codepoint <= 2472 ||
        codepoint >= 2474 && codepoint <= 2480 ||
        codepoint == 2482 ||
        codepoint >= 2486 && codepoint <= 2489 ||
        codepoint >= 2494 && codepoint <= 2496 ||
        codepoint >= 2503 && codepoint <= 2504 ||
        codepoint >= 2507 && codepoint <= 2508 ||
        codepoint == 2519 ||
        codepoint >= 2524 && codepoint <= 2525 ||
        codepoint >= 2527 && codepoint <= 2529 ||
        codepoint >= 2534 && codepoint <= 2545 ||
        codepoint >= 2548 && codepoint <= 2554 ||
        codepoint >= 2565 && codepoint <= 2570 ||
        codepoint >= 2575 && codepoint <= 2576 ||
        codepoint >= 2579 && codepoint <= 2600 ||
        codepoint >= 2602 && codepoint <= 2608 ||
        codepoint >= 2610 && codepoint <= 2611 ||
        codepoint >= 2613 && codepoint <= 2614 ||
        codepoint >= 2616 && codepoint <= 2617 ||
        codepoint >= 2622 && codepoint <= 2624 ||
        codepoint >= 2649 && codepoint <= 2652 ||
        codepoint == 2654 ||
        codepoint >= 2662 && codepoint <= 2671 ||
        codepoint >= 2674 && codepoint <= 2676 ||
        codepoint == 2691 ||
        codepoint >= 2693 && codepoint <= 2699 ||
        codepoint == 2701 ||
        codepoint >= 2703 && codepoint <= 2705 ||
        codepoint >= 2707 && codepoint <= 2728 ||
        codepoint >= 2730 && codepoint <= 2736 ||
        codepoint >= 2738 && codepoint <= 2739 ||
        codepoint >= 2741 && codepoint <= 2745 ||
        codepoint >= 2749 && codepoint <= 2752 ||
        codepoint == 2761 ||
        codepoint >= 2763 && codepoint <= 2764 ||
        codepoint == 2768 ||
        codepoint == 2784 ||
        codepoint >= 2790 && codepoint <= 2799 ||
        codepoint >= 2818 && codepoint <= 2819 ||
        codepoint >= 2821 && codepoint <= 2828 ||
        codepoint >= 2831 && codepoint <= 2832 ||
        codepoint >= 2835 && codepoint <= 2856 ||
        codepoint >= 2858 && codepoint <= 2864 ||
        codepoint >= 2866 && codepoint <= 2867 ||
        codepoint >= 2870 && codepoint <= 2873 ||
        codepoint >= 2877 && codepoint <= 2878 ||
        codepoint == 2880 ||
        codepoint >= 2887 && codepoint <= 2888 ||
        codepoint >= 2891 && codepoint <= 2892 ||
        codepoint == 2903 ||
        codepoint >= 2908 && codepoint <= 2909 ||
        codepoint >= 2911 && codepoint <= 2913 ||
        codepoint >= 2918 && codepoint <= 2928 ||
        codepoint == 2947 ||
        codepoint >= 2949 && codepoint <= 2954 ||
        codepoint >= 2958 && codepoint <= 2960 ||
        codepoint >= 2962 && codepoint <= 2965 ||
        codepoint >= 2969 && codepoint <= 2970 ||
        codepoint == 2972 ||
        codepoint >= 2974 && codepoint <= 2975 ||
        codepoint >= 2979 && codepoint <= 2980 ||
        codepoint >= 2984 && codepoint <= 2986 ||
        codepoint >= 2990 && codepoint <= 2997 ||
        codepoint >= 2999 && codepoint <= 3001 ||
        codepoint >= 3006 && codepoint <= 3007 ||
        codepoint >= 3009 && codepoint <= 3010 ||
        codepoint >= 3014 && codepoint <= 3016 ||
        codepoint >= 3018 && codepoint <= 3020 ||
        codepoint == 3031 ||
        codepoint >= 3047 && codepoint <= 3058 ||
        codepoint >= 3073 && codepoint <= 3075 ||
        codepoint >= 3077 && codepoint <= 3084 ||
        codepoint >= 3086 && codepoint <= 3088 ||
        codepoint >= 3090 && codepoint <= 3112 ||
        codepoint >= 3114 && codepoint <= 3123 ||
        codepoint >= 3125 && codepoint <= 3129 ||
        codepoint >= 3137 && codepoint <= 3140 ||
        codepoint >= 3168 && codepoint <= 3169 ||
        codepoint >= 3174 && codepoint <= 3183 ||
        codepoint >= 3202 && codepoint <= 3203 ||
        codepoint >= 3205 && codepoint <= 3212 ||
        codepoint >= 3214 && codepoint <= 3216 ||
        codepoint >= 3218 && codepoint <= 3240 ||
        codepoint >= 3242 && codepoint <= 3251 ||
        codepoint >= 3253 && codepoint <= 3257 ||
        codepoint == 3262 ||
        codepoint >= 3264 && codepoint <= 3268 ||
        codepoint >= 3271 && codepoint <= 3272 ||
        codepoint >= 3274 && codepoint <= 3275 ||
        codepoint >= 3285 && codepoint <= 3286 ||
        codepoint == 3294 ||
        codepoint >= 3296 && codepoint <= 3297 ||
        codepoint >= 3302 && codepoint <= 3311 ||
        codepoint >= 3330 && codepoint <= 3331 ||
        codepoint >= 3333 && codepoint <= 3340 ||
        codepoint >= 3342 && codepoint <= 3344 ||
        codepoint >= 3346 && codepoint <= 3368 ||
        codepoint >= 3370 && codepoint <= 3385 ||
        codepoint >= 3390 && codepoint <= 3392 ||
        codepoint >= 3398 && codepoint <= 3400 ||
        codepoint >= 3402 && codepoint <= 3404 ||
        codepoint == 3415 ||
        codepoint >= 3424 && codepoint <= 3425 ||
        codepoint >= 3430 && codepoint <= 3439 ||
        codepoint >= 3458 && codepoint <= 3459 ||
        codepoint >= 3461 && codepoint <= 3478 ||
        codepoint >= 3482 && codepoint <= 3505 ||
        codepoint >= 3507 && codepoint <= 3515 ||
        codepoint == 3517 ||
        codepoint >= 3520 && codepoint <= 3526 ||
        codepoint >= 3535 && codepoint <= 3537 ||
        codepoint >= 3544 && codepoint <= 3551 ||
        codepoint >= 3570 && codepoint <= 3572 ||
        codepoint >= 3585 && codepoint <= 3632 ||
        codepoint >= 3634 && codepoint <= 3635 ||
        codepoint >= 3648 && codepoint <= 3654 ||
        codepoint >= 3663 && codepoint <= 3675 ||
        codepoint >= 3713 && codepoint <= 3714 ||
        codepoint == 3716 ||
        codepoint >= 3719 && codepoint <= 3720 ||
        codepoint == 3722 ||
        codepoint == 3725 ||
        codepoint >= 3732 && codepoint <= 3735 ||
        codepoint >= 3737 && codepoint <= 3743 ||
        codepoint >= 3745 && codepoint <= 3747 ||
        codepoint == 3749 ||
        codepoint == 3751 ||
        codepoint >= 3754 && codepoint <= 3755 ||
        codepoint >= 3757 && codepoint <= 3760 ||
        codepoint >= 3762 && codepoint <= 3763 ||
        codepoint == 3773 ||
        codepoint >= 3776 && codepoint <= 3780 ||
        codepoint == 3782 ||
        codepoint >= 3792 && codepoint <= 3801 ||
        codepoint >= 3804 && codepoint <= 3805 ||
        codepoint >= 3840 && codepoint <= 3863 ||
        codepoint >= 3866 && codepoint <= 3892 ||
        codepoint == 3894 ||
        codepoint == 3896 ||
        codepoint >= 3902 && codepoint <= 3911 ||
        codepoint >= 3913 && codepoint <= 3946 ||
        codepoint == 3967 ||
        codepoint == 3973 ||
        codepoint >= 3976 && codepoint <= 3979 ||
        codepoint >= 4030 && codepoint <= 4037 ||
        codepoint >= 4039 && codepoint <= 4044 ||
        codepoint == 4047 ||
        codepoint >= 4096 && codepoint <= 4129 ||
        codepoint >= 4131 && codepoint <= 4135 ||
        codepoint >= 4137 && codepoint <= 4138 ||
        codepoint == 4140 ||
        codepoint == 4145 ||
        codepoint == 4152 ||
        codepoint >= 4160 && codepoint <= 4183 ||
        codepoint >= 4256 && codepoint <= 4293 ||
        codepoint >= 4304 && codepoint <= 4344 ||
        codepoint == 4347 ||
        codepoint >= 4352 && codepoint <= 4441 ||
        codepoint >= 4447 && codepoint <= 4514 ||
        codepoint >= 4520 && codepoint <= 4601 ||
        codepoint >= 4608 && codepoint <= 4614 ||
        codepoint >= 4616 && codepoint <= 4678 ||
        codepoint == 4680 ||
        codepoint >= 4682 && codepoint <= 4685 ||
        codepoint >= 4688 && codepoint <= 4694 ||
        codepoint == 4696 ||
        codepoint >= 4698 && codepoint <= 4701 ||
        codepoint >= 4704 && codepoint <= 4742 ||
        codepoint == 4744 ||
        codepoint >= 4746 && codepoint <= 4749 ||
        codepoint >= 4752 && codepoint <= 4782 ||
        codepoint == 4784 ||
        codepoint >= 4786 && codepoint <= 4789 ||
        codepoint >= 4792 && codepoint <= 4798 ||
        codepoint == 4800 ||
        codepoint >= 4802 && codepoint <= 4805 ||
        codepoint >= 4808 && codepoint <= 4814 ||
        codepoint >= 4816 && codepoint <= 4822 ||
        codepoint >= 4824 && codepoint <= 4846 ||
        codepoint >= 4848 && codepoint <= 4878 ||
        codepoint == 4880 ||
        codepoint >= 4882 && codepoint <= 4885 ||
        codepoint >= 4888 && codepoint <= 4894 ||
        codepoint >= 4896 && codepoint <= 4934 ||
        codepoint >= 4936 && codepoint <= 4954 ||
        codepoint >= 4961 && codepoint <= 4988 ||
        codepoint >= 5024 && codepoint <= 5108 ||
        codepoint >= 5121 && codepoint <= 5750 ||
        codepoint >= 5761 && codepoint <= 5786 ||
        codepoint >= 5792 && codepoint <= 5872 ||
        codepoint >= 5888 && codepoint <= 5900 ||
        codepoint >= 5902 && codepoint <= 5905 ||
        codepoint >= 5920 && codepoint <= 5937 ||
        codepoint >= 5941 && codepoint <= 5942 ||
        codepoint >= 5952 && codepoint <= 5969 ||
        codepoint >= 5984 && codepoint <= 5996 ||
        codepoint >= 5998 && codepoint <= 6000 ||
        codepoint >= 6016 && codepoint <= 6070 ||
        codepoint >= 6078 && codepoint <= 6085 ||
        codepoint >= 6087 && codepoint <= 6088 ||
        codepoint >= 6100 && codepoint <= 6106 ||
        codepoint == 6108 ||
        codepoint >= 6112 && codepoint <= 6121 ||
        codepoint >= 6160 && codepoint <= 6169 ||
        codepoint >= 6176 && codepoint <= 6263 ||
        codepoint >= 6272 && codepoint <= 6312 ||
        codepoint >= 7680 && codepoint <= 7835 ||
        codepoint >= 7840 && codepoint <= 7929 ||
        codepoint >= 7936 && codepoint <= 7957 ||
        codepoint >= 7960 && codepoint <= 7965 ||
        codepoint >= 7968 && codepoint <= 8005 ||
        codepoint >= 8008 && codepoint <= 8013 ||
        codepoint >= 8016 && codepoint <= 8023 ||
        codepoint == 8025 ||
        codepoint == 8027 ||
        codepoint == 8029 ||
        codepoint >= 8031 && codepoint <= 8061 ||
        codepoint >= 8064 && codepoint <= 8116 ||
        codepoint >= 8118 && codepoint <= 8124 ||
        codepoint == 8126 ||
        codepoint >= 8130 && codepoint <= 8132 ||
        codepoint >= 8134 && codepoint <= 8140 ||
        codepoint >= 8144 && codepoint <= 8147 ||
        codepoint >= 8150 && codepoint <= 8155 ||
        codepoint >= 8160 && codepoint <= 8172 ||
        codepoint >= 8178 && codepoint <= 8180 ||
        codepoint >= 8182 && codepoint <= 8188 ||
        codepoint == 8206 ||
        codepoint == 8305 ||
        codepoint == 8319 ||
        codepoint == 8450 ||
        codepoint == 8455 ||
        codepoint >= 8458 && codepoint <= 8467 ||
        codepoint == 8469 ||
        codepoint >= 8473 && codepoint <= 8477 ||
        codepoint == 8484 ||
        codepoint == 8486 ||
        codepoint == 8488 ||
        codepoint >= 8490 && codepoint <= 8493 ||
        codepoint >= 8495 && codepoint <= 8497 ||
        codepoint >= 8499 && codepoint <= 8505 ||
        codepoint >= 8509 && codepoint <= 8511 ||
        codepoint >= 8517 && codepoint <= 8521 ||
        codepoint >= 8544 && codepoint <= 8579 ||
        codepoint >= 9014 && codepoint <= 9082 ||
        codepoint == 9109 ||
        codepoint >= 9372 && codepoint <= 9449 ||
        codepoint >= 12293 && codepoint <= 12295 ||
        codepoint >= 12321 && codepoint <= 12329 ||
        codepoint >= 12337 && codepoint <= 12341 ||
        codepoint >= 12344 && codepoint <= 12348 ||
        codepoint >= 12353 && codepoint <= 12438 ||
        codepoint >= 12445 && codepoint <= 12447 ||
        codepoint >= 12449 && codepoint <= 12538 ||
        codepoint >= 12540 && codepoint <= 12543 ||
        codepoint >= 12549 && codepoint <= 12588 ||
        codepoint >= 12593 && codepoint <= 12686 ||
        codepoint >= 12688 && codepoint <= 12727 ||
        codepoint >= 12784 && codepoint <= 12828 ||
        codepoint >= 12832 && codepoint <= 12867 ||
        codepoint >= 12896 && codepoint <= 12923 ||
        codepoint >= 12927 && codepoint <= 12976 ||
        codepoint >= 12992 && codepoint <= 13003 ||
        codepoint >= 13008 && codepoint <= 13054 ||
        codepoint >= 13056 && codepoint <= 13174 ||
        codepoint >= 13179 && codepoint <= 13277 ||
        codepoint >= 13280 && codepoint <= 13310 ||
        codepoint >= 13312 && codepoint <= 19893 ||
        codepoint >= 19968 && codepoint <= 40869 ||
        codepoint >= 40960 && codepoint <= 42124 ||
        codepoint >= 44032 && codepoint <= 55203 ||
        codepoint >= 55296 && codepoint <= 64045 ||
        codepoint >= 64048 && codepoint <= 64106 ||
        codepoint >= 64256 && codepoint <= 64262 ||
        codepoint >= 64275 && codepoint <= 64279 ||
        codepoint >= 65313 && codepoint <= 65338 ||
        codepoint >= 65345 && codepoint <= 65370 ||
        codepoint >= 65382 && codepoint <= 65470 ||
        codepoint >= 65474 && codepoint <= 65479 ||
        codepoint >= 65482 && codepoint <= 65487 ||
        codepoint >= 65490 && codepoint <= 65495 ||
        codepoint >= 65498 && codepoint <= 65500 ||
        codepoint >= 66304 && codepoint <= 66334 ||
        codepoint >= 66336 && codepoint <= 66339 ||
        codepoint >= 66352 && codepoint <= 66378 ||
        codepoint >= 66560 && codepoint <= 66597 ||
        codepoint >= 66600 && codepoint <= 66637 ||
        codepoint >= 118784 && codepoint <= 119029 ||
        codepoint >= 119040 && codepoint <= 119078 ||
        codepoint >= 119082 && codepoint <= 119142 ||
        codepoint >= 119146 && codepoint <= 119154 ||
        codepoint >= 119171 && codepoint <= 119172 ||
        codepoint >= 119180 && codepoint <= 119209 ||
        codepoint >= 119214 && codepoint <= 119261 ||
        codepoint >= 119808 && codepoint <= 119892 ||
        codepoint >= 119894 && codepoint <= 119964 ||
        codepoint >= 119966 && codepoint <= 119967 ||
        codepoint == 119970 ||
        codepoint >= 119973 && codepoint <= 119974 ||
        codepoint >= 119977 && codepoint <= 119980 ||
        codepoint >= 119982 && codepoint <= 119993 ||
        codepoint == 119995 ||
        codepoint >= 119997 && codepoint <= 120000 ||
        codepoint >= 120002 && codepoint <= 120003 ||
        codepoint >= 120005 && codepoint <= 120069 ||
        codepoint >= 120071 && codepoint <= 120074 ||
        codepoint >= 120077 && codepoint <= 120084 ||
        codepoint >= 120086 && codepoint <= 120092 ||
        codepoint >= 120094 && codepoint <= 120121 ||
        codepoint >= 120123 && codepoint <= 120126 ||
        codepoint >= 120128 && codepoint <= 120132 ||
        codepoint == 120134 ||
        codepoint >= 120138 && codepoint <= 120144 ||
        codepoint >= 120146 && codepoint <= 120483 ||
        codepoint >= 120488 && codepoint <= 120777 ||
        codepoint >= 131072 && codepoint <= 173782 ||
        codepoint >= 194560 && codepoint <= 195101 ||
        codepoint >= 983040 && codepoint <= 1048573 ||
        codepoint >= 1048576 && codepoint <= 1114109
        ;
  }

  public static boolean bidirectional(List<Integer> value) throws IllegalArgumentException {
    boolean containPropertyRorAL = false;
    boolean firstCharacterPropertyRorAL = false;
    boolean lastCharacterPropertyRorAL = false;
    boolean containPropertyL = false;
    for (int i = 0; i < value.size(); i++) {
      int character = value.get(i);
      //1) The characters in section 5.8 MUST be prohibited.
      if (prohibitionChangeDisplayProperties(character)) {
        throw new IllegalArgumentException("Prohibited codepoint " + character + " at position " + i
            + " (unicode name: " + Character.getName(character) + ")");
      }
      if (bidirectionalPropertyRorAL(character)) {
        containPropertyRorAL = true;
        if (i == 0) {
          firstCharacterPropertyRorAL = true;
        }
        else if (i == value.size() - 1) {
          lastCharacterPropertyRorAL = true;
        }
      }
      if (bidirectionalPropertyL(character)) {
        containPropertyL = true;
      }
    }
    //2) If a string contains any RandALCat character,
    //the string MUST NOT contain any LCat character.
    if (containPropertyRorAL && containPropertyL) {
      throw new IllegalArgumentException("Prohibited string with RandALCat and LCat");
    }
    //3) If a string contains any RandALCat character, a RandALCat
    //character MUST be the first character of the string, and a
    //RandALCat character MUST be the last character of the string.
    if (containPropertyRorAL && !(firstCharacterPropertyRorAL && lastCharacterPropertyRorAL)) {
      throw new IllegalArgumentException("The string contains any RandALCat character "
          + "but a RandALCat character is not the first and the last characters");
    }
    return true;
  }
}
