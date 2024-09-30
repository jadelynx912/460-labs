package godb

//This file defines methods for working with tuples, including defining
// the types DBType, FieldType, TupleDesc, DBValue, and Tuple

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"strconv"
	"strings"
)

// DBType is the type of a tuple field, in GoDB, e.g., IntType or StringType
type DBType int

const (
	IntType     DBType = iota
	StringType  DBType = iota
	UnknownType DBType = iota //used internally, during parsing, because sometimes the type is unknown
)

func (t DBType) String() string {
	switch t {
	case IntType:
		return "int"
	case StringType:
		return "string"
	}
	return "unknown"
}

// FieldType is the type of a field in a tuple, e.g., its name, table, and [godb.DBType].
// TableQualifier may or may not be an emtpy string, depending on whether the table
// was specified in the query
type FieldType struct {
	Fname          string
	TableQualifier string
	Ftype          DBType
}

// TupleDesc is "type" of the tuple, e.g., the field names and types
type TupleDesc struct {
	Fields []FieldType
}

// Compare two tuple descs, and return true iff
// all of their field objects are equal and they
// are the same length
// TODO DONE: some code goes here
func (d1 *TupleDesc) equals(d2 *TupleDesc) bool {
	if len(d1.Fields) != len(d2.Fields) {
		return false
	}
	// Check the field objects
	for i, d1value := range d1.Fields {
		if d1value.Fname != d2.Fields[i].Fname {
			return false
		}
		if d1value.TableQualifier != d2.Fields[i].TableQualifier {
			return false
		}
		if d1value.Ftype.String() != d2.Fields[i].Ftype.String() {
			return false
		}
	}
	return true
}

// Hint: heap_page need function there:		//TODO DONE
func (desc *TupleDesc) bytesPerTuple() int {
	numBytes := 0
	for _, field := range desc.Fields {
		if field.Ftype == IntType { //FIXME double check this comparison works
			numBytes += 8 //for a  64 bit int /8 = 8 bytes
		} else if field.Ftype == StringType {
			numBytes += StringLength
		} else {
			// Handle other types or raise an error if needed
			return -1
		}
	}
	return numBytes
}

// Given a FieldType f and a TupleDesc desc, find the best
// matching field in desc for f.  A match is defined as
// having the same Ftype and the same name, preferring a match
// with the same TableQualifier if f has a TableQualifier
// We have provided this implementation because it's details are
// idiosyncratic to the behavior of the parser, which we are not
// asking you to write
func findFieldInTd(field FieldType, desc *TupleDesc) (int, error) {
	best := -1
	for i, f := range desc.Fields {
		if f.Fname == field.Fname && (f.Ftype == field.Ftype || field.Ftype == UnknownType) {
			if field.TableQualifier == "" && best != -1 {
				return 0, GoDBError{AmbiguousNameError, fmt.Sprintf("select name %s is ambiguous", f.Fname)}
			}
			if f.TableQualifier == field.TableQualifier || best == -1 {
				best = i
			}
		}
	}
	if best != -1 {
		return best, nil
	}
	return -1, GoDBError{IncompatibleTypesError, fmt.Sprintf("field %s.%s not found", field.TableQualifier, field.Fname)}

}

// Make a copy of a tuple desc.  Note that in go, assignment of a slice to
// another slice object does not make a copy of the contents of the slice.
// Look at the built-in function "copy".
// TODO DONE: some code goes here
func (td *TupleDesc) copy() *TupleDesc {
	output := make([]FieldType, len(td.Fields))
	numCopied := copy(output, td.Fields)
	if numCopied != len(td.Fields) {
		fmt.Print("ERROR copying tuple")
	}
	return &TupleDesc{
		Fields: output,
	}
}

// Assign the TableQualifier of every field in the TupleDesc to be the
// supplied alias.  We have provided this function as it is only used
// by the parser.
func (td *TupleDesc) setTableAlias(alias string) {
	fields := make([]FieldType, len(td.Fields))
	copy(fields, td.Fields)
	for i := range fields {
		fields[i].TableQualifier = alias
	}
	td.Fields = fields
}

// Merge two TupleDescs together.  The resulting TupleDesc
// should consist of the fields of desc2
// appended onto the fields of desc.
func (desc *TupleDesc) merge(desc2 *TupleDesc) *TupleDesc {
	// TODO DONE: some code goes here
	output := make([]FieldType, len(desc.Fields))
	copy(output, desc.Fields)
	output = append(output, desc2.Fields...)
	return &TupleDesc{
		Fields: output,
	} //replace me
}

// ================== Tuple Methods ======================

// Interface for tuple field values
type DBValue interface {
	EvalPred(v DBValue, op BoolOp) bool
}

// Integer field value
type IntField struct {
	Value int64
}

// String field value
type StringField struct {
	Value string
}

// Tuple represents the contents of a tuple read from a database
// It includes the tuple descriptor, and the value of the fields
type Tuple struct {
	Desc   TupleDesc
	Fields []DBValue
	Rid    recordID //used to track the page and position this page was read from
}

type recordID interface {
}

// Serialize the contents of the tuple into a byte array Since all tuples are of
// fixed size, this method should simply write the fields in sequential order
// into the supplied buffer.
//
// See the function [binary.Write].  Objects should be serialized in little
// endian oder.
//
// Strings can be converted to byte arrays by casting to []byte. Note that all
// strings need to be padded to StringLength bytes (set in types.go). For
// example if StringLength is set to 5, the string 'mit' should be written as
// 'm', 'i', 't', 0, 0
//
// May return an error if the buffer has insufficient capacity to store the
// tuple.
func (t *Tuple) writeTo(b *bytes.Buffer) error {
	// TODO DONE: some code goes here
	for _, val := range t.Fields {
		switch v := val.(type) {
		case IntField:
			err := binary.Write(b, binary.LittleEndian, v.Value)
			if err != nil {
				return fmt.Errorf("Error writing to buffer in writeTo()")
			}
		case StringField:
			padding_num := StringLength - len(v.Value)
			data := []byte(v.Value + strings.Repeat("0", padding_num))
			err := binary.Write(b, binary.LittleEndian, data)
			if err != nil {
				return fmt.Errorf("Error writing to buffer in writeTo()")
			}
		default:
			return fmt.Errorf("writeTo() something's wrong with the type of tuples in writeTo()")
		}
		// if t.Desc.Fields[i].Ftype == IntType {
		// 	// Need this for retyping - can't just do t.Fields[0].Value
		// 	f := t.Fields[i]
		// 	binary.Write(b, binary.LittleEndian, f.Value)
		// } else if t.Desc.Fields[i].Ftype == StringType {
		// 	f := t.Fields[i]
		// 	padding_num := StringLength - len(f.Value)
		// 	data := []byte(f.Value + strings.Repeat("0", padding_num))
		// 	binary.Write(b, binary.LittleEndian, data)
		// } else {
		// 	return fmt.Errorf("Something's wrong with the type of tuples in writeTo()")
		// }
	}
	return nil
	// return fmt.Errorf("writeTo not implemented") //replace me
}

// Read the contents of a tuple with the specified [TupleDesc] from the
// specified buffer, returning a Tuple.
//
// See [binary.Read]. Objects should be deserialized in little endian oder.
//
// All strings are stored as StringLength byte objects.
//
// Strings with length < StringLength will be padded with zeros, and these
// trailing zeros should be removed from the strings.  A []byte can be cast
// directly to string.
//
// May return an error if the buffer has insufficent data to deserialize the
// tuple.
func readTupleFrom(b *bytes.Buffer, desc *TupleDesc) (*Tuple, error) {
	// TODO DONE: some code goes here
	fields := make([]DBValue, 0)
	for _, field := range desc.Fields {
		if field.Ftype == IntType {
			var num int64
			err := binary.Read(b, binary.LittleEndian, &num)
			if err != nil {
				return nil, fmt.Errorf("Error reading from buffer")
			}
			i := IntField{Value: num}
			fields = append(fields, i)
		} else if field.Ftype == StringType {
			buf := make([]byte, StringLength)
			err := binary.Read(b, binary.LittleEndian, buf)
			if err != nil {
				return nil, fmt.Errorf("Error reading from buffer")
			}
			str := string(buf)
			trimmedStr := strings.TrimRight(str, "0")
			s := StringField{Value: trimmedStr}
			fields = append(fields, s)
		}
	}
	return &Tuple{
		Desc:   *desc,
		Fields: fields,
		Rid:    nil,
	}, nil
	// return nil, fmt.Errorf("readTupleFrom not implemented") //replace me
}

// Compare two tuples for equality.  Equality means that the TupleDescs are equal
// and all of the fields are equal.  TupleDescs should be compared with
// the [TupleDesc.equals] method, but fields can be compared directly with equality
// operators.
func (t1 *Tuple) equals(t2 *Tuple) bool {
	// TODO DONE: some code goes here
	if !(t1.Desc).equals(&t2.Desc) {
		return false
	}
	if len(t1.Fields) != len(t2.Fields) {
		return false
	}
	for i, val := range t1.Fields {
		val2 := t2.Fields[i]
		switch v1 := val.(type) {
		case IntField:
			v2, ok := val2.(IntField)
			if !ok || v1.Value != v2.Value {
				return false
			}
		case StringField:
			v2, ok := val2.(StringField)
			if !ok || v1.Value != v2.Value {
				return false
			}
		default:
			return false // Unsupported type
		}
		// if val.Value != t2.Fields[i].Value {
		// 	return false
		// }
	}
	return true
}

// Merge two tuples together, producing a new tuple with the fields of t2
// appended to t1. The new tuple should have a correct TupleDesc that is created
// by merging the descriptions of the two input tuples.
func joinTuples(t1 *Tuple, t2 *Tuple) *Tuple {
	// TODO DONE: some code goes here
	if t1 == nil { // Check since you can't reference Desc on a nil object
		return t2 // works if both are nil since (t2 nil), and nil should be returned
	}
	if t2 == nil {
		return t1
	}
	newDesc := t1.Desc.merge(&t2.Desc)
	newFields := make([]DBValue, len(t1.Fields))
	copy(newFields, t1.Fields)
	newFields = append(newFields, t2.Fields...)
	return &Tuple{
		Desc:   *newDesc,
		Fields: newFields,
		Rid:    nil, //FIXME unsure what to do about this - leave blank probably?
	} //replace me
}

type orderByState int

const (
	OrderedLessThan    orderByState = iota
	OrderedEqual       orderByState = iota
	OrderedGreaterThan orderByState = iota
)

// Apply the supplied expression to both t and t2, and compare the results,
// returning an orderByState value.
//
// Takes an arbitrary expressions rather than a field, because, e.g., for an
// ORDER BY SQL may ORDER BY arbitrary expressions, e.g., substr(name, 1, 2)
//
// Note that in most cases Expr will be a [godb.FieldExpr], which simply
// extracts a named field from a supplied tuple.
//
// Calling the [Expr.EvalExpr] method on a tuple will return the value of the
// expression on the supplied tuple.
//
// Note that EvalExpr uses the [Tuple.project] method, so you will need
// to implement projection before testing compareField.
func (t *Tuple) compareField(t2 *Tuple, field Expr) (orderByState, error) {
	// TODO: some code goes here
	valuet1, errt1 := field.EvalExpr(t)
	valuet2, errt2 := field.EvalExpr(t2)
	// For any Expr type, it returns a DBValue
	if errt1 != nil || errt2 != nil {
		return OrderedEqual, fmt.Errorf("Error evaluating expression")
	}
	switch v1 := valuet1.(type) {
	case IntField:
		v2, ok := valuet2.(IntField)
		if ok {
			if v1.Value < v2.Value {
				return OrderedLessThan, nil
			} else if v1.Value == v2.Value {
				return OrderedEqual, nil
			} else {
				return OrderedGreaterThan, nil
			}
		} else {
			return OrderedEqual, fmt.Errorf("compareField() - two fields not the same type ")
		}
	case StringField:
		v2, ok := valuet2.(StringField)
		if ok {
			if v1.Value < v2.Value {
				return OrderedLessThan, nil
			} else if v1.Value == v2.Value {
				return OrderedEqual, nil
			} else {
				return OrderedGreaterThan, nil
			}
		} else {
			return OrderedEqual, fmt.Errorf("compareField() - two fields not the same type ")
		}
	default:
		return OrderedEqual, fmt.Errorf("compareField() - t1 field unknown type ")
	}
	// if valuet1 < valuet2 {
	// 	return OrderedLessThan, nil
	// } else if valuet1 == valuet2 {
	// 	return OrderedEqual, nil
	// } else {
	// 	return OrderedGreaterThan, nil
	// }
	// return OrderedEqual, fmt.Errorf("compareField not implemented") // replace me
}

// Project out the supplied fields from the tuple. Should return a new Tuple
// with just the fields named in fields.
//
// Should not require a match on TableQualifier, but should prefer fields that
// do match on TableQualifier (e.g., a field  t1.name in fields should match an
// entry t2.name in t, but only if there is not an entry t1.name in t)
func (t *Tuple) project(fields []FieldType) (*Tuple, error) {
	// TODO DONE: some code goes here
	temp := []int{}
	data := []DBValue{}
	for _, field := range fields {
		for j, tupleField := range t.Desc.Fields {
			if field.Fname == tupleField.Fname {
				//temp = append(temp, tupleField)
				temp = append(temp, j)
			}
		}
		if len(temp) == 0 {
			return nil, fmt.Errorf("no matches found for a field in the tuple")
		} else if len(temp) > 1 { // >1 table with the same field name
			for _, val := range temp {
				if t.Desc.Fields[val].TableQualifier == field.TableQualifier {
					data = append(data, t.Fields[val])
					break
				}
			}
		} else {
			data = append(data, t.Fields[temp[0]])
		}

		temp = temp[:0] //Clear the temp slice
	}
	return &Tuple{Desc: TupleDesc{Fields: fields}, Fields: data, Rid: nil}, nil //replace me
}

// Compute a key for the tuple to be used in a map structure
func (t *Tuple) tupleKey() any {
	var buf bytes.Buffer
	t.writeTo(&buf)
	return buf.String()
}

var winWidth int = 120

func fmtCol(v string, ncols int) string {
	colWid := winWidth / ncols
	nextLen := len(v) + 3
	remLen := colWid - nextLen
	if remLen > 0 {
		spacesRight := remLen / 2
		spacesLeft := remLen - spacesRight
		return strings.Repeat(" ", spacesLeft) + v + strings.Repeat(" ", spacesRight) + " |"
	} else {
		return " " + v[0:colWid-4] + " |"
	}
}

// Return a string representing the header of a table for a tuple with the
// supplied TupleDesc.
//
// Aligned indicates if the tuple should be foramtted in a tabular format
func (d *TupleDesc) HeaderString(aligned bool) string {
	outstr := ""
	for i, f := range d.Fields {
		tableName := ""
		if f.TableQualifier != "" {
			tableName = f.TableQualifier + "."
		}

		if aligned {
			outstr = fmt.Sprintf("%s %s", outstr, fmtCol(tableName+f.Fname, len(d.Fields)))
		} else {
			sep := ","
			if i == 0 {
				sep = ""
			}
			outstr = fmt.Sprintf("%s%s%s", outstr, sep, tableName+f.Fname)
		}
	}
	return outstr
}

// Return a string representing the tuple
// Aligned indicates if the tuple should be formatted in a tabular format
func (t *Tuple) PrettyPrintString(aligned bool) string {
	outstr := ""
	for i, f := range t.Fields {
		str := ""
		switch f := f.(type) {
		case IntField:
			str = strconv.FormatInt(f.Value, 10)
		case StringField:
			str = f.Value
		}
		if aligned {
			outstr = fmt.Sprintf("%s %s", outstr, fmtCol(str, len(t.Fields)))
		} else {
			sep := ","
			if i == 0 {
				sep = ""
			}
			outstr = fmt.Sprintf("%s%s%s", outstr, sep, str)
		}
	}
	return outstr
}
