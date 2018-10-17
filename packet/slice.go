package packet

//段，若干个byte组成
type Field struct {
	Index    int  //在Slice中的序号，可能为负
	Size     int  //长度>=0
	Start    int  //开始位置（包含），可能为负
	Stop     int  //结束位置（不包含），可能为负
	Optional bool //可选或不定长度（非固定）
}

func NewField(size int, optional bool) *Field {
	start := 0
	if size < 0 {
		start = size
		size = 0 - size //取绝对值
	}
	return &Field{
		Index: 0, Size: size,
		Start: start, Stop: 0,
		Optional: optional,
	}
}

//分段匹配
type FieldSlice struct {
	Sequence []*Field //开头已定义段
	Reverse  []*Field //结尾已定义段
	Rest     *Field   //未识别部分，可作为payload创建新包
}

func NewFieldSlice() *FieldSlice {
	rest := NewField(0, false) //初始时，全部字节未识别
	return &FieldSlice{Rest: rest}
}

func (obj *FieldSlice) GetLeastSize() int {
	least := 0
	for _, f := range obj.Sequence {
		least += f.Size
	}
	if len(obj.Reverse) > 0 {
		for _, f := range obj.Reverse {
			least += f.Size
		}
	}
	return least
}

//添加开头的段定义
func (obj *FieldSlice) AddField(field *Field) *Field {
	field.Index = len(obj.Sequence)
	obj.Rest.Index = field.Index + 1
	field.Start += obj.Rest.Start
	if field.Size > 0 {
		field.Stop = field.Start + field.Size
		if !field.Optional { //增加固定字段时，缩减未知部分的范围
			obj.Rest.Start = field.Stop
		}
	}
	obj.Sequence = append(obj.Sequence, field)
	return field
}

//添加结尾的段定义
func (obj *FieldSlice) AddRevField(field *Field) *Field {
	field.Index = -len(obj.Reverse) - 1
	field.Start += obj.Rest.Stop
	if field.Size > 0 {
		field.Stop = field.Start + field.Size
		if !field.Optional { //缩减未知部分的范围
			obj.Rest.Stop = field.Start
		}
	}
	obj.Reverse = append(obj.Reverse, field)
	return field
}

//找出段的正向起止位置，offset为修正值，只对同符号数据起作用
func (obj *FieldSlice) GetFieldRange(field *Field, length, offset int) (int, int) {
	var (
		start = field.Start
		stop  = field.Stop
	)
	if start*offset > 0 { //皆为正或皆为负，加上修正值
		start += offset
	}
	if start < 0 { //负数转为正向，Go的slice索引不支持负数
		start += length
	}
	if stop*offset >= 0 { //同符号时修正
		stop += offset
	}
	if stop <= 0 { //负数转为正向
		stop += length
	}
	return start, stop
}

//根据段索引找出段的定义
func (obj *FieldSlice) GetFieldByIndex(index int) *Field {
	var (
		schLen = len(obj.Sequence)
		revLen = len(obj.Reverse)
		length = schLen + revLen
	)
	if index > length || index < -length {
		return nil //索引超出范围
	}
	if index == schLen || index == -revLen-1 {
		return obj.Rest //未识别部分
	}
	if revLen == 0 { //没有结尾段定义，情况较简单
		if index < 0 {
			index += length
		}
		return obj.Sequence[index]
	} else if index >= 0 && index < schLen {
		return obj.Sequence[index]
	} else if index < -revLen-1 {
		return obj.Sequence[index+length+1]
	} else if index > schLen {
		return obj.Reverse[index-length-1]
	} else {
		return obj.Reverse[-index-1]
	}
}

//直接定义并读取开头几个固定段，类似Erlang中的位匹配
func (obj *FieldSlice) MatchFixeds(payload []byte, fieldSizes []int) [][]byte {
	var (
		field       *Field
		start, stop int
		result      [][]byte
	)
	for _, size := range fieldSizes {
		field = obj.AddField(NewField(size, false))
		start, stop = obj.GetFieldRange(field, len(payload), 0)
		result = append(result, payload[start:stop])
	}
	return result
}
