package tools

/*
* use similar text as text compare engine
* add scores with every level words
* Created by vitoxie
* Date 2018-12-30
* Email xiepaup@163.com
 */

type SmartSimilarityText struct {
}

func NewSmartSimilarityText() *SmartSimilarityText {
	return &SmartSimilarityText{}
}

/*
*use SmartIdentifyText can have higher performance and Accruance !
*ef :
*    a := "tdw|112729|20181219|djfsd982lsd289jdksfj0flksadfjsdf2lkfsadh9fasddfyf"
*    b := "tdw|112729|20190323|9dkfk892o3kd9sdfa9dfakdfj92fklsdahf;as"
*  SimilarText ---> output is 40
*  but SmartIdentifyText ---> output is 74
 */

// return a int value in [0, 100], which stands for match level
func (this *SmartSimilarityText) SimilarText(str1, str2 string) int {

	txt1, txt2 := []rune(str1), []rune(str2)

	if len(txt1) == 0 || len(txt2) == 0 {
		return 0
	}
	return this.similarChar(txt1, txt2) * 200 / (len(txt1) + len(txt2))
}

// return a int value in [0, 100], which stands for match level
func (this *SmartSimilarityText) SmartIdentifyText(s1, s2 string) int {
	var sim int

	words1 := this.SmartDevideText(s1)
	words2 := this.SmartDevideText(s2)

	if (len(words1) == 0 && len(words2) == 0) || len(words1) !=
		len(words2) {
		//fmt.Println("two string with different segs ")
		// or downgrade to compare original engine ... ; for faster we just ignored this
		return 0
	}

	weightScores := this.smartWeights(len(words1))

	for i := 0; i < len(words1); i++ {
		sameval := this.SimilarText(words1[i], words2[i])
		//fmt.Printf("words : %s, %s similary : %d\n", words1[i], words2[i], sameval)
		sim += sameval * weightScores[i]
	}

	return sim / 100
}

func (this *SmartSimilarityText) smartWeights(l int) []int {

	if l <= 0 {
		return nil
	}

	avgscore := 100 / l
	middle := l / 2

	var scores []int
	for i := 0; i < l; i++ {
		scores = append(scores, avgscore+middle*(middle-i))
	}

	return scores
}

// return the len of longest string both in str1 and str2 and the positions in str1 and str2
func (this *SmartSimilarityText) similarStr(str1 []rune, str2 []rune) (int, int, int) {
	var maxLen, tmp, pos1, pos2 = 0, 0, 0, 0
	len1, len2 := len(str1), len(str2)

	for p := 0; p < len1; p++ {
		for q := 0; q < len2; q++ {
			tmp = 0
			for p+tmp < len1 && q+tmp < len2 && str1[p+tmp] == str2[q+tmp] {
				tmp++
			}
			if tmp > maxLen {
				maxLen, pos1, pos2 = tmp, p, q
			}
		}

	}

	return maxLen, pos1, pos2
}

// return the total length of longest string both in str1 and str2
func (this *SmartSimilarityText) similarChar(str1 []rune, str2 []rune) int {
	maxLen, pos1, pos2 := this.similarStr(str1, str2)
	total := maxLen

	if maxLen != 0 {
		if pos1 > 0 && pos2 > 0 {
			total += this.similarChar(str1[:pos1], str2[:pos2])
		}
		if pos1+maxLen < len(str1) && pos2+maxLen < len(str2) {
			total += this.similarChar(str1[pos1+maxLen:], str2[pos2+maxLen:])
		}
	}

	return total
}

func SmartDevideText(s string) []string {
	// TODO
	return strings.Split(s, "|")
}
