// Code generated by protoc-gen-gogo. DO NOT EDIT.
// source: metapb.proto

package metapb

import (
	"fmt"
	"io"
	"math"

	proto "github.com/golang/protobuf/proto"

	_ "github.com/gogo/protobuf/gogoproto"
)

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.ProtoPackageIsVersion2 // please upgrade the proto package

type StoreState int32

const (
	StoreState_Up        StoreState = 0
	StoreState_Offline   StoreState = 1
	StoreState_Tombstone StoreState = 2
)

var StoreState_name = map[int32]string{
	0: "Up",
	1: "Offline",
	2: "Tombstone",
}
var StoreState_value = map[string]int32{
	"Up":        0,
	"Offline":   1,
	"Tombstone": 2,
}

func (x StoreState) String() string {
	return proto.EnumName(StoreState_name, int32(x))
}
func (StoreState) EnumDescriptor() ([]byte, []int) {
	return fileDescriptor_metapb_33de520265e54ab4, []int{0}
}

type Cluster struct {
	Id uint64 `protobuf:"varint,1,opt,name=id,proto3" json:"id,omitempty"`
	// max peer count for a region.
	// scheduler will do the auto-balance if region peer count mismatches.
	MaxPeerCount         uint32   `protobuf:"varint,2,opt,name=max_peer_count,json=maxPeerCount,proto3" json:"max_peer_count,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *Cluster) Reset()         { *m = Cluster{} }
func (m *Cluster) String() string { return proto.CompactTextString(m) }
func (*Cluster) ProtoMessage()    {}
func (*Cluster) Descriptor() ([]byte, []int) {
	return fileDescriptor_metapb_33de520265e54ab4, []int{0}
}
func (m *Cluster) XXX_Unmarshal(b []byte) error {
	return m.Unmarshal(b)
}
func (m *Cluster) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	if deterministic {
		return xxx_messageInfo_Cluster.Marshal(b, m, deterministic)
	} else {
		b = b[:cap(b)]
		n, err := m.MarshalTo(b)
		if err != nil {
			return nil, err
		}
		return b[:n], nil
	}
}
func (dst *Cluster) XXX_Merge(src proto.Message) {
	xxx_messageInfo_Cluster.Merge(dst, src)
}
func (m *Cluster) XXX_Size() int {
	return m.Size()
}
func (m *Cluster) XXX_DiscardUnknown() {
	xxx_messageInfo_Cluster.DiscardUnknown(m)
}

var xxx_messageInfo_Cluster proto.InternalMessageInfo

func (m *Cluster) GetId() uint64 {
	if m != nil {
		return m.Id
	}
	return 0
}

func (m *Cluster) GetMaxPeerCount() uint32 {
	if m != nil {
		return m.MaxPeerCount
	}
	return 0
}

type Store struct {
	Id uint64 `protobuf:"varint,1,opt,name=id,proto3" json:"id,omitempty"`
	// Address to handle client requests (kv, cop, etc.)
	Address              string     `protobuf:"bytes,2,opt,name=address,proto3" json:"address,omitempty"`
	State                StoreState `protobuf:"varint,3,opt,name=state,proto3,enum=metapb.StoreState" json:"state,omitempty"`
	XXX_NoUnkeyedLiteral struct{}   `json:"-"`
	XXX_unrecognized     []byte     `json:"-"`
	XXX_sizecache        int32      `json:"-"`
}

func (m *Store) Reset()         { *m = Store{} }
func (m *Store) String() string { return proto.CompactTextString(m) }
func (*Store) ProtoMessage()    {}
func (*Store) Descriptor() ([]byte, []int) {
	return fileDescriptor_metapb_33de520265e54ab4, []int{1}
}
func (m *Store) XXX_Unmarshal(b []byte) error {
	return m.Unmarshal(b)
}
func (m *Store) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	if deterministic {
		return xxx_messageInfo_Store.Marshal(b, m, deterministic)
	} else {
		b = b[:cap(b)]
		n, err := m.MarshalTo(b)
		if err != nil {
			return nil, err
		}
		return b[:n], nil
	}
}
func (dst *Store) XXX_Merge(src proto.Message) {
	xxx_messageInfo_Store.Merge(dst, src)
}
func (m *Store) XXX_Size() int {
	return m.Size()
}
func (m *Store) XXX_DiscardUnknown() {
	xxx_messageInfo_Store.DiscardUnknown(m)
}

var xxx_messageInfo_Store proto.InternalMessageInfo

func (m *Store) GetId() uint64 {
	if m != nil {
		return m.Id
	}
	return 0
}

func (m *Store) GetAddress() string {
	if m != nil {
		return m.Address
	}
	return ""
}

func (m *Store) GetState() StoreState {
	if m != nil {
		return m.State
	}
	return StoreState_Up
}

type RegionEpoch struct {
	// Conf change version, auto increment when add or remove peer
	ConfVer uint64 `protobuf:"varint,1,opt,name=conf_ver,json=confVer,proto3" json:"conf_ver,omitempty"`
	// Region version, auto increment when split or merge
	Version              uint64   `protobuf:"varint,2,opt,name=version,proto3" json:"version,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *RegionEpoch) Reset()         { *m = RegionEpoch{} }
func (m *RegionEpoch) String() string { return proto.CompactTextString(m) }
func (*RegionEpoch) ProtoMessage()    {}
func (*RegionEpoch) Descriptor() ([]byte, []int) {
	return fileDescriptor_metapb_33de520265e54ab4, []int{2}
}
func (m *RegionEpoch) XXX_Unmarshal(b []byte) error {
	return m.Unmarshal(b)
}
func (m *RegionEpoch) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	if deterministic {
		return xxx_messageInfo_RegionEpoch.Marshal(b, m, deterministic)
	} else {
		b = b[:cap(b)]
		n, err := m.MarshalTo(b)
		if err != nil {
			return nil, err
		}
		return b[:n], nil
	}
}
func (dst *RegionEpoch) XXX_Merge(src proto.Message) {
	xxx_messageInfo_RegionEpoch.Merge(dst, src)
}
func (m *RegionEpoch) XXX_Size() int {
	return m.Size()
}
func (m *RegionEpoch) XXX_DiscardUnknown() {
	xxx_messageInfo_RegionEpoch.DiscardUnknown(m)
}

var xxx_messageInfo_RegionEpoch proto.InternalMessageInfo

func (m *RegionEpoch) GetConfVer() uint64 {
	if m != nil {
		return m.ConfVer
	}
	return 0
}

func (m *RegionEpoch) GetVersion() uint64 {
	if m != nil {
		return m.Version
	}
	return 0
}

type Region struct {
	Id uint64 `protobuf:"varint,1,opt,name=id,proto3" json:"id,omitempty"`
	// Region key range [start_key, end_key).
	StartKey             []byte       `protobuf:"bytes,2,opt,name=start_key,json=startKey,proto3" json:"start_key,omitempty"`
	EndKey               []byte       `protobuf:"bytes,3,opt,name=end_key,json=endKey,proto3" json:"end_key,omitempty"`
	RegionEpoch          *RegionEpoch `protobuf:"bytes,4,opt,name=region_epoch,json=regionEpoch" json:"region_epoch,omitempty"`
	Peers                []*Peer      `protobuf:"bytes,5,rep,name=peers" json:"peers,omitempty"`
	XXX_NoUnkeyedLiteral struct{}     `json:"-"`
	XXX_unrecognized     []byte       `json:"-"`
	XXX_sizecache        int32        `json:"-"`
}

func (m *Region) Reset()         { *m = Region{} }
func (m *Region) String() string { return proto.CompactTextString(m) }
func (*Region) ProtoMessage()    {}
func (*Region) Descriptor() ([]byte, []int) {
	return fileDescriptor_metapb_33de520265e54ab4, []int{3}
}
func (m *Region) XXX_Unmarshal(b []byte) error {
	return m.Unmarshal(b)
}
func (m *Region) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	if deterministic {
		return xxx_messageInfo_Region.Marshal(b, m, deterministic)
	} else {
		b = b[:cap(b)]
		n, err := m.MarshalTo(b)
		if err != nil {
			return nil, err
		}
		return b[:n], nil
	}
}
func (dst *Region) XXX_Merge(src proto.Message) {
	xxx_messageInfo_Region.Merge(dst, src)
}
func (m *Region) XXX_Size() int {
	return m.Size()
}
func (m *Region) XXX_DiscardUnknown() {
	xxx_messageInfo_Region.DiscardUnknown(m)
}

var xxx_messageInfo_Region proto.InternalMessageInfo

func (m *Region) GetId() uint64 {
	if m != nil {
		return m.Id
	}
	return 0
}

func (m *Region) GetStartKey() []byte {
	if m != nil {
		return m.StartKey
	}
	return nil
}

func (m *Region) GetEndKey() []byte {
	if m != nil {
		return m.EndKey
	}
	return nil
}

func (m *Region) GetRegionEpoch() *RegionEpoch {
	if m != nil {
		return m.RegionEpoch
	}
	return nil
}

func (m *Region) GetPeers() []*Peer {
	if m != nil {
		return m.Peers
	}
	return nil
}

type Peer struct {
	Id                   uint64   `protobuf:"varint,1,opt,name=id,proto3" json:"id,omitempty"`
	StoreId              uint64   `protobuf:"varint,2,opt,name=store_id,json=storeId,proto3" json:"store_id,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *Peer) Reset()         { *m = Peer{} }
func (m *Peer) String() string { return proto.CompactTextString(m) }
func (*Peer) ProtoMessage()    {}
func (*Peer) Descriptor() ([]byte, []int) {
	return fileDescriptor_metapb_33de520265e54ab4, []int{4}
}
func (m *Peer) XXX_Unmarshal(b []byte) error {
	return m.Unmarshal(b)
}
func (m *Peer) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	if deterministic {
		return xxx_messageInfo_Peer.Marshal(b, m, deterministic)
	} else {
		b = b[:cap(b)]
		n, err := m.MarshalTo(b)
		if err != nil {
			return nil, err
		}
		return b[:n], nil
	}
}
func (dst *Peer) XXX_Merge(src proto.Message) {
	xxx_messageInfo_Peer.Merge(dst, src)
}
func (m *Peer) XXX_Size() int {
	return m.Size()
}
func (m *Peer) XXX_DiscardUnknown() {
	xxx_messageInfo_Peer.DiscardUnknown(m)
}

var xxx_messageInfo_Peer proto.InternalMessageInfo

func (m *Peer) GetId() uint64 {
	if m != nil {
		return m.Id
	}
	return 0
}

func (m *Peer) GetStoreId() uint64 {
	if m != nil {
		return m.StoreId
	}
	return 0
}

func init() {
	proto.RegisterType((*Cluster)(nil), "metapb.Cluster")
	proto.RegisterType((*Store)(nil), "metapb.Store")
	proto.RegisterType((*RegionEpoch)(nil), "metapb.RegionEpoch")
	proto.RegisterType((*Region)(nil), "metapb.Region")
	proto.RegisterType((*Peer)(nil), "metapb.Peer")
	proto.RegisterEnum("metapb.StoreState", StoreState_name, StoreState_value)
}
func (m *Cluster) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalTo(dAtA)
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *Cluster) MarshalTo(dAtA []byte) (int, error) {
	var i int
	_ = i
	var l int
	_ = l
	if m.Id != 0 {
		dAtA[i] = 0x8
		i++
		i = encodeVarintMetapb(dAtA, i, uint64(m.Id))
	}
	if m.MaxPeerCount != 0 {
		dAtA[i] = 0x10
		i++
		i = encodeVarintMetapb(dAtA, i, uint64(m.MaxPeerCount))
	}
	if m.XXX_unrecognized != nil {
		i += copy(dAtA[i:], m.XXX_unrecognized)
	}
	return i, nil
}

func (m *Store) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalTo(dAtA)
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *Store) MarshalTo(dAtA []byte) (int, error) {
	var i int
	_ = i
	var l int
	_ = l
	if m.Id != 0 {
		dAtA[i] = 0x8
		i++
		i = encodeVarintMetapb(dAtA, i, uint64(m.Id))
	}
	if len(m.Address) > 0 {
		dAtA[i] = 0x12
		i++
		i = encodeVarintMetapb(dAtA, i, uint64(len(m.Address)))
		i += copy(dAtA[i:], m.Address)
	}
	if m.State != 0 {
		dAtA[i] = 0x18
		i++
		i = encodeVarintMetapb(dAtA, i, uint64(m.State))
	}
	if m.XXX_unrecognized != nil {
		i += copy(dAtA[i:], m.XXX_unrecognized)
	}
	return i, nil
}

func (m *RegionEpoch) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalTo(dAtA)
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *RegionEpoch) MarshalTo(dAtA []byte) (int, error) {
	var i int
	_ = i
	var l int
	_ = l
	if m.ConfVer != 0 {
		dAtA[i] = 0x8
		i++
		i = encodeVarintMetapb(dAtA, i, uint64(m.ConfVer))
	}
	if m.Version != 0 {
		dAtA[i] = 0x10
		i++
		i = encodeVarintMetapb(dAtA, i, uint64(m.Version))
	}
	if m.XXX_unrecognized != nil {
		i += copy(dAtA[i:], m.XXX_unrecognized)
	}
	return i, nil
}

func (m *Region) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalTo(dAtA)
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *Region) MarshalTo(dAtA []byte) (int, error) {
	var i int
	_ = i
	var l int
	_ = l
	if m.Id != 0 {
		dAtA[i] = 0x8
		i++
		i = encodeVarintMetapb(dAtA, i, uint64(m.Id))
	}
	if len(m.StartKey) > 0 {
		dAtA[i] = 0x12
		i++
		i = encodeVarintMetapb(dAtA, i, uint64(len(m.StartKey)))
		i += copy(dAtA[i:], m.StartKey)
	}
	if len(m.EndKey) > 0 {
		dAtA[i] = 0x1a
		i++
		i = encodeVarintMetapb(dAtA, i, uint64(len(m.EndKey)))
		i += copy(dAtA[i:], m.EndKey)
	}
	if m.RegionEpoch != nil {
		dAtA[i] = 0x22
		i++
		i = encodeVarintMetapb(dAtA, i, uint64(m.RegionEpoch.Size()))
		n1, err := m.RegionEpoch.MarshalTo(dAtA[i:])
		if err != nil {
			return 0, err
		}
		i += n1
	}
	if len(m.Peers) > 0 {
		for _, msg := range m.Peers {
			dAtA[i] = 0x2a
			i++
			i = encodeVarintMetapb(dAtA, i, uint64(msg.Size()))
			n, err := msg.MarshalTo(dAtA[i:])
			if err != nil {
				return 0, err
			}
			i += n
		}
	}
	if m.XXX_unrecognized != nil {
		i += copy(dAtA[i:], m.XXX_unrecognized)
	}
	return i, nil
}

func (m *Peer) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalTo(dAtA)
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *Peer) MarshalTo(dAtA []byte) (int, error) {
	var i int
	_ = i
	var l int
	_ = l
	if m.Id != 0 {
		dAtA[i] = 0x8
		i++
		i = encodeVarintMetapb(dAtA, i, uint64(m.Id))
	}
	if m.StoreId != 0 {
		dAtA[i] = 0x10
		i++
		i = encodeVarintMetapb(dAtA, i, uint64(m.StoreId))
	}
	if m.XXX_unrecognized != nil {
		i += copy(dAtA[i:], m.XXX_unrecognized)
	}
	return i, nil
}

func encodeVarintMetapb(dAtA []byte, offset int, v uint64) int {
	for v >= 1<<7 {
		dAtA[offset] = uint8(v&0x7f | 0x80)
		v >>= 7
		offset++
	}
	dAtA[offset] = uint8(v)
	return offset + 1
}
func (m *Cluster) Size() (n int) {
	var l int
	_ = l
	if m.Id != 0 {
		n += 1 + sovMetapb(uint64(m.Id))
	}
	if m.MaxPeerCount != 0 {
		n += 1 + sovMetapb(uint64(m.MaxPeerCount))
	}
	if m.XXX_unrecognized != nil {
		n += len(m.XXX_unrecognized)
	}
	return n
}

func (m *Store) Size() (n int) {
	var l int
	_ = l
	if m.Id != 0 {
		n += 1 + sovMetapb(uint64(m.Id))
	}
	l = len(m.Address)
	if l > 0 {
		n += 1 + l + sovMetapb(uint64(l))
	}
	if m.State != 0 {
		n += 1 + sovMetapb(uint64(m.State))
	}
	if m.XXX_unrecognized != nil {
		n += len(m.XXX_unrecognized)
	}
	return n
}

func (m *RegionEpoch) Size() (n int) {
	var l int
	_ = l
	if m.ConfVer != 0 {
		n += 1 + sovMetapb(uint64(m.ConfVer))
	}
	if m.Version != 0 {
		n += 1 + sovMetapb(uint64(m.Version))
	}
	if m.XXX_unrecognized != nil {
		n += len(m.XXX_unrecognized)
	}
	return n
}

func (m *Region) Size() (n int) {
	var l int
	_ = l
	if m.Id != 0 {
		n += 1 + sovMetapb(uint64(m.Id))
	}
	l = len(m.StartKey)
	if l > 0 {
		n += 1 + l + sovMetapb(uint64(l))
	}
	l = len(m.EndKey)
	if l > 0 {
		n += 1 + l + sovMetapb(uint64(l))
	}
	if m.RegionEpoch != nil {
		l = m.RegionEpoch.Size()
		n += 1 + l + sovMetapb(uint64(l))
	}
	if len(m.Peers) > 0 {
		for _, e := range m.Peers {
			l = e.Size()
			n += 1 + l + sovMetapb(uint64(l))
		}
	}
	if m.XXX_unrecognized != nil {
		n += len(m.XXX_unrecognized)
	}
	return n
}

func (m *Peer) Size() (n int) {
	var l int
	_ = l
	if m.Id != 0 {
		n += 1 + sovMetapb(uint64(m.Id))
	}
	if m.StoreId != 0 {
		n += 1 + sovMetapb(uint64(m.StoreId))
	}
	if m.XXX_unrecognized != nil {
		n += len(m.XXX_unrecognized)
	}
	return n
}

func sovMetapb(x uint64) (n int) {
	for {
		n++
		x >>= 7
		if x == 0 {
			break
		}
	}
	return n
}
func sozMetapb(x uint64) (n int) {
	return sovMetapb(uint64((x << 1) ^ uint64((int64(x) >> 63))))
}
func (m *Cluster) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowMetapb
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= (uint64(b) & 0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: Cluster: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: Cluster: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field Id", wireType)
			}
			m.Id = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowMetapb
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.Id |= (uint64(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		case 2:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field MaxPeerCount", wireType)
			}
			m.MaxPeerCount = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowMetapb
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.MaxPeerCount |= (uint32(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		default:
			iNdEx = preIndex
			skippy, err := skipMetapb(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if skippy < 0 {
				return ErrInvalidLengthMetapb
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			m.XXX_unrecognized = append(m.XXX_unrecognized, dAtA[iNdEx:iNdEx+skippy]...)
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func (m *Store) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowMetapb
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= (uint64(b) & 0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: Store: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: Store: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field Id", wireType)
			}
			m.Id = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowMetapb
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.Id |= (uint64(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		case 2:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Address", wireType)
			}
			var stringLen uint64
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowMetapb
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				stringLen |= (uint64(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			intStringLen := int(stringLen)
			if intStringLen < 0 {
				return ErrInvalidLengthMetapb
			}
			postIndex := iNdEx + intStringLen
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Address = string(dAtA[iNdEx:postIndex])
			iNdEx = postIndex
		case 3:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field State", wireType)
			}
			m.State = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowMetapb
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.State |= (StoreState(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		default:
			iNdEx = preIndex
			skippy, err := skipMetapb(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if skippy < 0 {
				return ErrInvalidLengthMetapb
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			m.XXX_unrecognized = append(m.XXX_unrecognized, dAtA[iNdEx:iNdEx+skippy]...)
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func (m *RegionEpoch) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowMetapb
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= (uint64(b) & 0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: RegionEpoch: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: RegionEpoch: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field ConfVer", wireType)
			}
			m.ConfVer = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowMetapb
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.ConfVer |= (uint64(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		case 2:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field Version", wireType)
			}
			m.Version = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowMetapb
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.Version |= (uint64(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		default:
			iNdEx = preIndex
			skippy, err := skipMetapb(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if skippy < 0 {
				return ErrInvalidLengthMetapb
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			m.XXX_unrecognized = append(m.XXX_unrecognized, dAtA[iNdEx:iNdEx+skippy]...)
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func (m *Region) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowMetapb
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= (uint64(b) & 0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: Region: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: Region: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field Id", wireType)
			}
			m.Id = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowMetapb
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.Id |= (uint64(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		case 2:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field StartKey", wireType)
			}
			var byteLen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowMetapb
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				byteLen |= (int(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if byteLen < 0 {
				return ErrInvalidLengthMetapb
			}
			postIndex := iNdEx + byteLen
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.StartKey = append(m.StartKey[:0], dAtA[iNdEx:postIndex]...)
			if m.StartKey == nil {
				m.StartKey = []byte{}
			}
			iNdEx = postIndex
		case 3:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field EndKey", wireType)
			}
			var byteLen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowMetapb
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				byteLen |= (int(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if byteLen < 0 {
				return ErrInvalidLengthMetapb
			}
			postIndex := iNdEx + byteLen
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.EndKey = append(m.EndKey[:0], dAtA[iNdEx:postIndex]...)
			if m.EndKey == nil {
				m.EndKey = []byte{}
			}
			iNdEx = postIndex
		case 4:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field RegionEpoch", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowMetapb
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= (int(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msglen < 0 {
				return ErrInvalidLengthMetapb
			}
			postIndex := iNdEx + msglen
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			if m.RegionEpoch == nil {
				m.RegionEpoch = &RegionEpoch{}
			}
			if err := m.RegionEpoch.Unmarshal(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
		case 5:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Peers", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowMetapb
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= (int(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msglen < 0 {
				return ErrInvalidLengthMetapb
			}
			postIndex := iNdEx + msglen
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Peers = append(m.Peers, &Peer{})
			if err := m.Peers[len(m.Peers)-1].Unmarshal(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := skipMetapb(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if skippy < 0 {
				return ErrInvalidLengthMetapb
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			m.XXX_unrecognized = append(m.XXX_unrecognized, dAtA[iNdEx:iNdEx+skippy]...)
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func (m *Peer) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowMetapb
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= (uint64(b) & 0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: Peer: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: Peer: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field Id", wireType)
			}
			m.Id = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowMetapb
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.Id |= (uint64(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		case 2:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field StoreId", wireType)
			}
			m.StoreId = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowMetapb
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.StoreId |= (uint64(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		default:
			iNdEx = preIndex
			skippy, err := skipMetapb(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if skippy < 0 {
				return ErrInvalidLengthMetapb
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			m.XXX_unrecognized = append(m.XXX_unrecognized, dAtA[iNdEx:iNdEx+skippy]...)
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func skipMetapb(dAtA []byte) (n int, err error) {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return 0, ErrIntOverflowMetapb
			}
			if iNdEx >= l {
				return 0, io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= (uint64(b) & 0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		wireType := int(wire & 0x7)
		switch wireType {
		case 0:
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return 0, ErrIntOverflowMetapb
				}
				if iNdEx >= l {
					return 0, io.ErrUnexpectedEOF
				}
				iNdEx++
				if dAtA[iNdEx-1] < 0x80 {
					break
				}
			}
			return iNdEx, nil
		case 1:
			iNdEx += 8
			return iNdEx, nil
		case 2:
			var length int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return 0, ErrIntOverflowMetapb
				}
				if iNdEx >= l {
					return 0, io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				length |= (int(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			iNdEx += length
			if length < 0 {
				return 0, ErrInvalidLengthMetapb
			}
			return iNdEx, nil
		case 3:
			for {
				var innerWire uint64
				var start int = iNdEx
				for shift := uint(0); ; shift += 7 {
					if shift >= 64 {
						return 0, ErrIntOverflowMetapb
					}
					if iNdEx >= l {
						return 0, io.ErrUnexpectedEOF
					}
					b := dAtA[iNdEx]
					iNdEx++
					innerWire |= (uint64(b) & 0x7F) << shift
					if b < 0x80 {
						break
					}
				}
				innerWireType := int(innerWire & 0x7)
				if innerWireType == 4 {
					break
				}
				next, err := skipMetapb(dAtA[start:])
				if err != nil {
					return 0, err
				}
				iNdEx = start + next
			}
			return iNdEx, nil
		case 4:
			return iNdEx, nil
		case 5:
			iNdEx += 4
			return iNdEx, nil
		default:
			return 0, fmt.Errorf("proto: illegal wireType %d", wireType)
		}
	}
	panic("unreachable")
}

var (
	ErrInvalidLengthMetapb = fmt.Errorf("proto: negative length found during unmarshaling")
	ErrIntOverflowMetapb   = fmt.Errorf("proto: integer overflow")
)

func init() { proto.RegisterFile("metapb.proto", fileDescriptor_metapb_33de520265e54ab4) }

var fileDescriptor_metapb_33de520265e54ab4 = []byte{
	// 390 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0x64, 0x52, 0xdd, 0x8a, 0xd3, 0x40,
	0x18, 0xdd, 0x49, 0xf3, 0xd3, 0x7e, 0xc9, 0x96, 0x30, 0x0a, 0x66, 0x15, 0x42, 0x08, 0x5e, 0x04,
	0x2f, 0x56, 0xad, 0xe0, 0xad, 0xb0, 0x8b, 0x17, 0xe2, 0x85, 0x32, 0xab, 0xde, 0x78, 0x11, 0xd2,
	0xce, 0xd7, 0x1a, 0xdc, 0xcc, 0x84, 0x99, 0xd9, 0xa5, 0x7d, 0x13, 0x9f, 0xc1, 0x27, 0xf1, 0xd2,
	0x47, 0x90, 0xfa, 0x22, 0x32, 0x93, 0x06, 0x0b, 0xbd, 0xcb, 0xf9, 0x4e, 0xce, 0x77, 0xce, 0x77,
	0x18, 0x48, 0x3a, 0x34, 0x4d, 0xbf, 0xbc, 0xec, 0x95, 0x34, 0x92, 0x86, 0x03, 0x7a, 0xfc, 0x70,
	0x23, 0x37, 0xd2, 0x8d, 0x9e, 0xdb, 0xaf, 0x81, 0x2d, 0xdf, 0x40, 0x74, 0x7d, 0x7b, 0xa7, 0x0d,
	0x2a, 0x3a, 0x07, 0xaf, 0xe5, 0x19, 0x29, 0x48, 0xe5, 0x33, 0xaf, 0xe5, 0xf4, 0x29, 0xcc, 0xbb,
	0x66, 0x5b, 0xf7, 0x88, 0xaa, 0x5e, 0xc9, 0x3b, 0x61, 0x32, 0xaf, 0x20, 0xd5, 0x39, 0x4b, 0xba,
	0x66, 0xfb, 0x11, 0x51, 0x5d, 0xdb, 0x59, 0xf9, 0x15, 0x82, 0x1b, 0x23, 0x15, 0x9e, 0xc8, 0x33,
	0x88, 0x1a, 0xce, 0x15, 0x6a, 0xed, 0x74, 0x33, 0x36, 0x42, 0x5a, 0x41, 0xa0, 0x4d, 0x63, 0x30,
	0x9b, 0x14, 0xa4, 0x9a, 0x2f, 0xe8, 0xe5, 0x21, 0xaf, 0xdb, 0x73, 0x63, 0x19, 0x36, 0xfc, 0x50,
	0x5e, 0x41, 0xcc, 0x70, 0xd3, 0x4a, 0xf1, 0xb6, 0x97, 0xab, 0x6f, 0xf4, 0x02, 0xa6, 0x2b, 0x29,
	0xd6, 0xf5, 0x3d, 0xaa, 0x83, 0x51, 0x64, 0xf1, 0x17, 0x54, 0xd6, 0xed, 0x1e, 0x95, 0x6e, 0xa5,
	0x70, 0x6e, 0x3e, 0x1b, 0x61, 0xf9, 0x93, 0x40, 0x38, 0x2c, 0x39, 0x89, 0xf8, 0x04, 0x66, 0xda,
	0x34, 0xca, 0xd4, 0xdf, 0x71, 0xe7, 0x64, 0x09, 0x9b, 0xba, 0xc1, 0x7b, 0xdc, 0xd1, 0x47, 0x10,
	0xa1, 0xe0, 0x8e, 0x9a, 0x38, 0x2a, 0x44, 0xc1, 0x2d, 0xf1, 0x1a, 0x12, 0xe5, 0xf6, 0xd5, 0x68,
	0x53, 0x65, 0x7e, 0x41, 0xaa, 0x78, 0xf1, 0x60, 0xbc, 0xe2, 0x28, 0x30, 0x8b, 0xd5, 0x51, 0xfa,
	0x12, 0x02, 0xdb, 0xa5, 0xce, 0x82, 0x62, 0x52, 0xc5, 0x8b, 0x64, 0x14, 0xd8, 0x2e, 0xd9, 0x40,
	0x95, 0x2f, 0xc1, 0xb7, 0xf0, 0x24, 0xe9, 0x05, 0x4c, 0xb5, 0x6d, 0xa7, 0x6e, 0xf9, 0x78, 0x9f,
	0xc3, 0xef, 0xf8, 0xb3, 0x17, 0x00, 0xff, 0x8b, 0xa3, 0x21, 0x78, 0x9f, 0xfb, 0xf4, 0x8c, 0xc6,
	0x10, 0x7d, 0x58, 0xaf, 0x6f, 0x5b, 0x81, 0x29, 0xa1, 0xe7, 0x30, 0xfb, 0x24, 0xbb, 0xa5, 0x36,
	0x52, 0x60, 0xea, 0x5d, 0xa5, 0xbf, 0xf6, 0x39, 0xf9, 0xbd, 0xcf, 0xc9, 0x9f, 0x7d, 0x4e, 0x7e,
	0xfc, 0xcd, 0xcf, 0x96, 0xa1, 0x7b, 0x0c, 0xaf, 0xfe, 0x05, 0x00, 0x00, 0xff, 0xff, 0x77, 0xf7,
	0x52, 0x80, 0x3a, 0x02, 0x00, 0x00,
}