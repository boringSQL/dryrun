package query

import (
	pg_query "github.com/pganalyze/pg_query_go/v6"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// maps a variant's message name to its Node oneof field, so bare messages
// (SelectStmt.Larg is a *SelectStmt, not a *Node) can be re-wrapped
var (
	nodeOneof    protoreflect.OneofDescriptor
	nodeVariants map[protoreflect.FullName]protoreflect.FieldDescriptor
)

func init() {
	md := (&pg_query.Node{}).ProtoReflect().Descriptor()
	nodeOneof = md.Oneofs().Get(0)

	nodeVariants = make(map[protoreflect.FullName]protoreflect.FieldDescriptor, md.Fields().Len())
	fields := md.Fields()
	for i := 0; i < fields.Len(); i++ {
		fd := fields.Get(i)
		if fd.Kind() == protoreflect.MessageKind {
			nodeVariants[fd.Message().FullName()] = fd
		}
	}
}

// targets, not reads: SELECT ... INTO, and FOR UPDATE OF (RangeVars holding aliases)
var skipWalkFields = map[protoreflect.Name]bool{
	"into_clause":    true,
	"locking_clause": true,
}

// Reflection, not a hand-listed switch: that one kept missing node types
// (USING, FILTER, OVER, ORDER BY, RETURNING). Pre-order, so marks a callback
// sets on a parent (SubLink -> its SelectStmt) land before the children.
func walkNode(node *pg_query.Node, fn func(*pg_query.Node)) {
	if node == nil {
		return
	}
	fn(node)

	rn := node.ProtoReflect()
	active := rn.WhichOneof(nodeOneof)
	if active == nil {
		return
	}
	inner := rn.Get(active).Message()
	if !inner.IsValid() {
		return
	}
	// SET (a, b) = (SELECT ...): the parser hangs one source on every column's
	// MultiAssignRef; walking it per column duplicates locs and breaks rewrites
	if m, ok := inner.Interface().(*pg_query.MultiAssignRef); ok && m.Colno > 1 {
		return
	}
	walkFields(inner, fn)
}

// by declared index, not Range, so Tables order is deterministic
func walkFields(msg protoreflect.Message, fn func(*pg_query.Node)) {
	fields := msg.Descriptor().Fields()
	for i := 0; i < fields.Len(); i++ {
		fd := fields.Get(i)
		if fd.Kind() != protoreflect.MessageKind || skipWalkFields[fd.Name()] {
			continue
		}
		if !msg.Has(fd) {
			continue
		}
		val := msg.Get(fd)
		if fd.IsList() {
			list := val.List()
			for j := 0; j < list.Len(); j++ {
				walkChild(list.Get(j).Message(), fn)
			}
			continue
		}
		walkChild(val.Message(), fn)
	}
}

// the wrap keeps the child's pointer: callers key maps by *SelectStmt identity
func walkChild(child protoreflect.Message, fn func(*pg_query.Node)) {
	if n, ok := child.Interface().(*pg_query.Node); ok {
		walkNode(n, fn)
		return
	}
	if fd, ok := nodeVariants[child.Descriptor().FullName()]; ok {
		wrapper := &pg_query.Node{}
		wrapper.ProtoReflect().Set(fd, protoreflect.ValueOfMessage(child))
		walkNode(wrapper, fn)
		return
	}
	walkFields(child, fn)
}
