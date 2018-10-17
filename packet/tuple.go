package packet

//带名称的切片
type NamedTuple struct {
	Slicer     *FieldSlice
	Locates    map[string]int       //记录名称对应的段索引
	Conditions map[int](func() int) //记录可选或不定长字段的判定方法
	//优化修正值的计算，跳过开头或结尾连续的固定字段
	condStart int
	condStop  int
}

func NewNamedTuple() *NamedTuple {
	return &NamedTuple{
		Slicer:     NewFieldSlice(),
		Locates:    make(map[string]int),
		Conditions: make(map[int](func() int)),
	}
}

//增加几个固定字段（size>0或size<0）
func (obj *NamedTuple) AddFixeds(size int, names ...string) {
	var field *Field
	for _, name := range names {
		//size<0时取绝对值，并导致start<0
		field = NewField(size, false)
		if field.Start < 0 {
			field = obj.Slicer.AddRevField(field)
		} else {
			field = obj.Slicer.AddField(field)
		}
		obj.Locates[name] = field.Index
	}
}

//增加一个变长（size=0）或可选字段，及其判定长度的方法
func (obj *NamedTuple) AddOptional(size int, name string, cond func() int) {
	var field = NewField(size, true)
	if field.Start < 0 {
		field = obj.Slicer.AddRevField(field)
		if obj.condStop == 0 {
			obj.condStop = field.Index
		}
	} else {
		field = obj.Slicer.AddField(field)
		if obj.condStart == 0 {
			obj.condStart = field.Index
		}
	}
	obj.Locates[name] = field.Index
	obj.Conditions[field.Index] = cond
}

//执行判定条件，得出非固定字段的长度
func (obj *NamedTuple) ExecCondition(idx int) int {
	if cond, ok := obj.Conditions[idx]; ok {
		return cond()
	}
	return 0
}

//前面有非固定字段时，需要计算修正值
func (obj *NamedTuple) CalcOptionalOffset(field *Field) int {
	if len(obj.Conditions) == 0 {
		return 0
	}
	var offset int = 0
	if field.Index >= 0 {
		for i := obj.condStart; i < field.Index; i++ {
			if f := obj.Slicer.Sequence[i]; f.Optional {
				offset += obj.ExecCondition(i)
			}
		}
	} else {
		for i := obj.condStop; i > field.Index; i-- {
			if f := obj.Slicer.Reverse[i]; f.Optional {
				offset -= obj.ExecCondition(i)
			}
		}
	}
	return offset
}

func (obj *NamedTuple) GetRest() *Field {
	return obj.Slicer.Rest
}

func (obj *NamedTuple) GetField(name string) *Field {
	if index, ok := obj.Locates[name]; ok {
		return obj.Slicer.GetFieldByIndex(index)
	}
	return nil
}

func (obj *NamedTuple) GetRealRange(field *Field, length int) (int, int) {
	if field.Optional && obj.ExecCondition(field.Index) == 0 {
		return 0, 0 //非固定字段有定义，但没有数据
	}
	offset := obj.CalcOptionalOffset(field)
	return obj.Slicer.GetFieldRange(field, length, offset)
}
