// Code generated by protoc-gen-gogo. DO NOT EDIT.
// source: share/shwap/pb/shwap_pb.proto

package shwap_pb

import (
	fmt "fmt"
	pb "github.com/celestiaorg/celestia-node/share/pb"
	proto "github.com/gogo/protobuf/proto"
	io "io"
	math "math"
	math_bits "math/bits"
)

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.GoGoProtoPackageIsVersion3 // please upgrade the proto package

type RowResponse struct {
	RowId []byte  `protobuf:"bytes,1,opt,name=row_id,json=rowId,proto3" json:"row_id,omitempty"`
	Row   *pb.Row `protobuf:"bytes,2,opt,name=row,proto3" json:"row,omitempty"`
}

func (m *RowResponse) Reset()         { *m = RowResponse{} }
func (m *RowResponse) String() string { return proto.CompactTextString(m) }
func (*RowResponse) ProtoMessage()    {}
func (*RowResponse) Descriptor() ([]byte, []int) {
	return fileDescriptor_fdfe0676a85dc852, []int{0}
}
func (m *RowResponse) XXX_Unmarshal(b []byte) error {
	return m.Unmarshal(b)
}
func (m *RowResponse) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	if deterministic {
		return xxx_messageInfo_RowResponse.Marshal(b, m, deterministic)
	} else {
		b = b[:cap(b)]
		n, err := m.MarshalToSizedBuffer(b)
		if err != nil {
			return nil, err
		}
		return b[:n], nil
	}
}
func (m *RowResponse) XXX_Merge(src proto.Message) {
	xxx_messageInfo_RowResponse.Merge(m, src)
}
func (m *RowResponse) XXX_Size() int {
	return m.Size()
}
func (m *RowResponse) XXX_DiscardUnknown() {
	xxx_messageInfo_RowResponse.DiscardUnknown(m)
}

var xxx_messageInfo_RowResponse proto.InternalMessageInfo

func (m *RowResponse) GetRowId() []byte {
	if m != nil {
		return m.RowId
	}
	return nil
}

func (m *RowResponse) GetRow() *pb.Row {
	if m != nil {
		return m.Row
	}
	return nil
}

type SampleResponse struct {
	SampleId []byte             `protobuf:"bytes,1,opt,name=sample_id,json=sampleId,proto3" json:"sample_id,omitempty"`
	Sample   *pb.ShareWithProof `protobuf:"bytes,2,opt,name=sample,proto3" json:"sample,omitempty"`
}

func (m *SampleResponse) Reset()         { *m = SampleResponse{} }
func (m *SampleResponse) String() string { return proto.CompactTextString(m) }
func (*SampleResponse) ProtoMessage()    {}
func (*SampleResponse) Descriptor() ([]byte, []int) {
	return fileDescriptor_fdfe0676a85dc852, []int{1}
}
func (m *SampleResponse) XXX_Unmarshal(b []byte) error {
	return m.Unmarshal(b)
}
func (m *SampleResponse) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	if deterministic {
		return xxx_messageInfo_SampleResponse.Marshal(b, m, deterministic)
	} else {
		b = b[:cap(b)]
		n, err := m.MarshalToSizedBuffer(b)
		if err != nil {
			return nil, err
		}
		return b[:n], nil
	}
}
func (m *SampleResponse) XXX_Merge(src proto.Message) {
	xxx_messageInfo_SampleResponse.Merge(m, src)
}
func (m *SampleResponse) XXX_Size() int {
	return m.Size()
}
func (m *SampleResponse) XXX_DiscardUnknown() {
	xxx_messageInfo_SampleResponse.DiscardUnknown(m)
}

var xxx_messageInfo_SampleResponse proto.InternalMessageInfo

func (m *SampleResponse) GetSampleId() []byte {
	if m != nil {
		return m.SampleId
	}
	return nil
}

func (m *SampleResponse) GetSample() *pb.ShareWithProof {
	if m != nil {
		return m.Sample
	}
	return nil
}

type DataResponse struct {
	DataId []byte   `protobuf:"bytes,1,opt,name=data_id,json=dataId,proto3" json:"data_id,omitempty"`
	Data   *pb.Data `protobuf:"bytes,2,opt,name=data,proto3" json:"data,omitempty"`
}

func (m *DataResponse) Reset()         { *m = DataResponse{} }
func (m *DataResponse) String() string { return proto.CompactTextString(m) }
func (*DataResponse) ProtoMessage()    {}
func (*DataResponse) Descriptor() ([]byte, []int) {
	return fileDescriptor_fdfe0676a85dc852, []int{2}
}
func (m *DataResponse) XXX_Unmarshal(b []byte) error {
	return m.Unmarshal(b)
}
func (m *DataResponse) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	if deterministic {
		return xxx_messageInfo_DataResponse.Marshal(b, m, deterministic)
	} else {
		b = b[:cap(b)]
		n, err := m.MarshalToSizedBuffer(b)
		if err != nil {
			return nil, err
		}
		return b[:n], nil
	}
}
func (m *DataResponse) XXX_Merge(src proto.Message) {
	xxx_messageInfo_DataResponse.Merge(m, src)
}
func (m *DataResponse) XXX_Size() int {
	return m.Size()
}
func (m *DataResponse) XXX_DiscardUnknown() {
	xxx_messageInfo_DataResponse.DiscardUnknown(m)
}

var xxx_messageInfo_DataResponse proto.InternalMessageInfo

func (m *DataResponse) GetDataId() []byte {
	if m != nil {
		return m.DataId
	}
	return nil
}

func (m *DataResponse) GetData() *pb.Data {
	if m != nil {
		return m.Data
	}
	return nil
}

func init() {
	proto.RegisterType((*RowResponse)(nil), "RowResponse")
	proto.RegisterType((*SampleResponse)(nil), "SampleResponse")
	proto.RegisterType((*DataResponse)(nil), "DataResponse")
}

func init() { proto.RegisterFile("share/shwap/pb/shwap_pb.proto", fileDescriptor_fdfe0676a85dc852) }

var fileDescriptor_fdfe0676a85dc852 = []byte{
	// 238 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0xe2, 0x92, 0x2d, 0xce, 0x48, 0x2c,
	0x4a, 0xd5, 0x2f, 0xce, 0x28, 0x4f, 0x2c, 0xd0, 0x2f, 0x48, 0x82, 0x30, 0xe2, 0x0b, 0x92, 0xf4,
	0x0a, 0x8a, 0xf2, 0x4b, 0xf2, 0xa5, 0xb8, 0x4b, 0x2a, 0x0b, 0x52, 0x8b, 0x21, 0x1c, 0x25, 0x1b,
	0x2e, 0xee, 0xa0, 0xfc, 0xf2, 0xa0, 0xd4, 0xe2, 0x82, 0xfc, 0xbc, 0xe2, 0x54, 0x21, 0x51, 0x2e,
	0xb6, 0xa2, 0xfc, 0xf2, 0xf8, 0xcc, 0x14, 0x09, 0x46, 0x05, 0x46, 0x0d, 0x9e, 0x20, 0xd6, 0xa2,
	0xfc, 0x72, 0xcf, 0x14, 0x21, 0x31, 0x2e, 0xe6, 0xa2, 0xfc, 0x72, 0x09, 0x26, 0x05, 0x46, 0x0d,
	0x6e, 0x23, 0x16, 0x3d, 0x90, 0x0e, 0x90, 0x80, 0x52, 0x18, 0x17, 0x5f, 0x70, 0x62, 0x6e, 0x41,
	0x4e, 0x2a, 0xdc, 0x00, 0x69, 0x2e, 0xce, 0x62, 0xb0, 0x08, 0xc2, 0x0c, 0x0e, 0x88, 0x80, 0x67,
	0x8a, 0x90, 0x3a, 0x17, 0x1b, 0x84, 0x0d, 0x35, 0x89, 0x5f, 0x2f, 0x18, 0xe4, 0xd2, 0xf0, 0xcc,
	0x92, 0x8c, 0x80, 0xa2, 0xfc, 0xfc, 0xb4, 0x20, 0xa8, 0xb4, 0x92, 0x13, 0x17, 0x8f, 0x4b, 0x62,
	0x49, 0x22, 0xdc, 0x54, 0x71, 0x2e, 0xf6, 0x94, 0xc4, 0x92, 0x44, 0x84, 0x99, 0x6c, 0x20, 0xae,
	0x67, 0x8a, 0x90, 0x24, 0x17, 0x0b, 0x88, 0x05, 0x35, 0x8f, 0x55, 0x0f, 0xac, 0x0b, 0x2c, 0xe4,
	0x24, 0x71, 0xe2, 0x91, 0x1c, 0xe3, 0x85, 0x47, 0x72, 0x8c, 0x0f, 0x1e, 0xc9, 0x31, 0x4e, 0x78,
	0x2c, 0xc7, 0x70, 0xe1, 0xb1, 0x1c, 0xc3, 0x8d, 0xc7, 0x72, 0x0c, 0x49, 0x6c, 0x60, 0xaf, 0x1b,
	0x03, 0x02, 0x00, 0x00, 0xff, 0xff, 0x0b, 0xcd, 0x17, 0x98, 0x28, 0x01, 0x00, 0x00,
}

func (m *RowResponse) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalToSizedBuffer(dAtA[:size])
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *RowResponse) MarshalTo(dAtA []byte) (int, error) {
	size := m.Size()
	return m.MarshalToSizedBuffer(dAtA[:size])
}

func (m *RowResponse) MarshalToSizedBuffer(dAtA []byte) (int, error) {
	i := len(dAtA)
	_ = i
	var l int
	_ = l
	if m.Row != nil {
		{
			size, err := m.Row.MarshalToSizedBuffer(dAtA[:i])
			if err != nil {
				return 0, err
			}
			i -= size
			i = encodeVarintShwapPb(dAtA, i, uint64(size))
		}
		i--
		dAtA[i] = 0x12
	}
	if len(m.RowId) > 0 {
		i -= len(m.RowId)
		copy(dAtA[i:], m.RowId)
		i = encodeVarintShwapPb(dAtA, i, uint64(len(m.RowId)))
		i--
		dAtA[i] = 0xa
	}
	return len(dAtA) - i, nil
}

func (m *SampleResponse) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalToSizedBuffer(dAtA[:size])
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *SampleResponse) MarshalTo(dAtA []byte) (int, error) {
	size := m.Size()
	return m.MarshalToSizedBuffer(dAtA[:size])
}

func (m *SampleResponse) MarshalToSizedBuffer(dAtA []byte) (int, error) {
	i := len(dAtA)
	_ = i
	var l int
	_ = l
	if m.Sample != nil {
		{
			size, err := m.Sample.MarshalToSizedBuffer(dAtA[:i])
			if err != nil {
				return 0, err
			}
			i -= size
			i = encodeVarintShwapPb(dAtA, i, uint64(size))
		}
		i--
		dAtA[i] = 0x12
	}
	if len(m.SampleId) > 0 {
		i -= len(m.SampleId)
		copy(dAtA[i:], m.SampleId)
		i = encodeVarintShwapPb(dAtA, i, uint64(len(m.SampleId)))
		i--
		dAtA[i] = 0xa
	}
	return len(dAtA) - i, nil
}

func (m *DataResponse) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalToSizedBuffer(dAtA[:size])
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *DataResponse) MarshalTo(dAtA []byte) (int, error) {
	size := m.Size()
	return m.MarshalToSizedBuffer(dAtA[:size])
}

func (m *DataResponse) MarshalToSizedBuffer(dAtA []byte) (int, error) {
	i := len(dAtA)
	_ = i
	var l int
	_ = l
	if m.Data != nil {
		{
			size, err := m.Data.MarshalToSizedBuffer(dAtA[:i])
			if err != nil {
				return 0, err
			}
			i -= size
			i = encodeVarintShwapPb(dAtA, i, uint64(size))
		}
		i--
		dAtA[i] = 0x12
	}
	if len(m.DataId) > 0 {
		i -= len(m.DataId)
		copy(dAtA[i:], m.DataId)
		i = encodeVarintShwapPb(dAtA, i, uint64(len(m.DataId)))
		i--
		dAtA[i] = 0xa
	}
	return len(dAtA) - i, nil
}

func encodeVarintShwapPb(dAtA []byte, offset int, v uint64) int {
	offset -= sovShwapPb(v)
	base := offset
	for v >= 1<<7 {
		dAtA[offset] = uint8(v&0x7f | 0x80)
		v >>= 7
		offset++
	}
	dAtA[offset] = uint8(v)
	return base
}
func (m *RowResponse) Size() (n int) {
	if m == nil {
		return 0
	}
	var l int
	_ = l
	l = len(m.RowId)
	if l > 0 {
		n += 1 + l + sovShwapPb(uint64(l))
	}
	if m.Row != nil {
		l = m.Row.Size()
		n += 1 + l + sovShwapPb(uint64(l))
	}
	return n
}

func (m *SampleResponse) Size() (n int) {
	if m == nil {
		return 0
	}
	var l int
	_ = l
	l = len(m.SampleId)
	if l > 0 {
		n += 1 + l + sovShwapPb(uint64(l))
	}
	if m.Sample != nil {
		l = m.Sample.Size()
		n += 1 + l + sovShwapPb(uint64(l))
	}
	return n
}

func (m *DataResponse) Size() (n int) {
	if m == nil {
		return 0
	}
	var l int
	_ = l
	l = len(m.DataId)
	if l > 0 {
		n += 1 + l + sovShwapPb(uint64(l))
	}
	if m.Data != nil {
		l = m.Data.Size()
		n += 1 + l + sovShwapPb(uint64(l))
	}
	return n
}

func sovShwapPb(x uint64) (n int) {
	return (math_bits.Len64(x|1) + 6) / 7
}
func sozShwapPb(x uint64) (n int) {
	return sovShwapPb(uint64((x << 1) ^ uint64((int64(x) >> 63))))
}
func (m *RowResponse) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowShwapPb
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: RowResponse: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: RowResponse: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field RowId", wireType)
			}
			var byteLen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowShwapPb
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				byteLen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if byteLen < 0 {
				return ErrInvalidLengthShwapPb
			}
			postIndex := iNdEx + byteLen
			if postIndex < 0 {
				return ErrInvalidLengthShwapPb
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.RowId = append(m.RowId[:0], dAtA[iNdEx:postIndex]...)
			if m.RowId == nil {
				m.RowId = []byte{}
			}
			iNdEx = postIndex
		case 2:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Row", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowShwapPb
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msglen < 0 {
				return ErrInvalidLengthShwapPb
			}
			postIndex := iNdEx + msglen
			if postIndex < 0 {
				return ErrInvalidLengthShwapPb
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			if m.Row == nil {
				m.Row = &pb.Row{}
			}
			if err := m.Row.Unmarshal(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := skipShwapPb(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return ErrInvalidLengthShwapPb
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func (m *SampleResponse) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowShwapPb
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: SampleResponse: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: SampleResponse: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field SampleId", wireType)
			}
			var byteLen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowShwapPb
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				byteLen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if byteLen < 0 {
				return ErrInvalidLengthShwapPb
			}
			postIndex := iNdEx + byteLen
			if postIndex < 0 {
				return ErrInvalidLengthShwapPb
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.SampleId = append(m.SampleId[:0], dAtA[iNdEx:postIndex]...)
			if m.SampleId == nil {
				m.SampleId = []byte{}
			}
			iNdEx = postIndex
		case 2:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Sample", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowShwapPb
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msglen < 0 {
				return ErrInvalidLengthShwapPb
			}
			postIndex := iNdEx + msglen
			if postIndex < 0 {
				return ErrInvalidLengthShwapPb
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			if m.Sample == nil {
				m.Sample = &pb.ShareWithProof{}
			}
			if err := m.Sample.Unmarshal(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := skipShwapPb(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return ErrInvalidLengthShwapPb
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func (m *DataResponse) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowShwapPb
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: DataResponse: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: DataResponse: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field DataId", wireType)
			}
			var byteLen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowShwapPb
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				byteLen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if byteLen < 0 {
				return ErrInvalidLengthShwapPb
			}
			postIndex := iNdEx + byteLen
			if postIndex < 0 {
				return ErrInvalidLengthShwapPb
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.DataId = append(m.DataId[:0], dAtA[iNdEx:postIndex]...)
			if m.DataId == nil {
				m.DataId = []byte{}
			}
			iNdEx = postIndex
		case 2:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Data", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowShwapPb
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msglen < 0 {
				return ErrInvalidLengthShwapPb
			}
			postIndex := iNdEx + msglen
			if postIndex < 0 {
				return ErrInvalidLengthShwapPb
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			if m.Data == nil {
				m.Data = &pb.Data{}
			}
			if err := m.Data.Unmarshal(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := skipShwapPb(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return ErrInvalidLengthShwapPb
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func skipShwapPb(dAtA []byte) (n int, err error) {
	l := len(dAtA)
	iNdEx := 0
	depth := 0
	for iNdEx < l {
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return 0, ErrIntOverflowShwapPb
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
					return 0, ErrIntOverflowShwapPb
				}
				if iNdEx >= l {
					return 0, io.ErrUnexpectedEOF
				}
				iNdEx++
				if dAtA[iNdEx-1] < 0x80 {
					break
				}
			}
		case 1:
			iNdEx += 8
		case 2:
			var length int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return 0, ErrIntOverflowShwapPb
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
			if length < 0 {
				return 0, ErrInvalidLengthShwapPb
			}
			iNdEx += length
		case 3:
			depth++
		case 4:
			if depth == 0 {
				return 0, ErrUnexpectedEndOfGroupShwapPb
			}
			depth--
		case 5:
			iNdEx += 4
		default:
			return 0, fmt.Errorf("proto: illegal wireType %d", wireType)
		}
		if iNdEx < 0 {
			return 0, ErrInvalidLengthShwapPb
		}
		if depth == 0 {
			return iNdEx, nil
		}
	}
	return 0, io.ErrUnexpectedEOF
}

var (
	ErrInvalidLengthShwapPb        = fmt.Errorf("proto: negative length found during unmarshaling")
	ErrIntOverflowShwapPb          = fmt.Errorf("proto: integer overflow")
	ErrUnexpectedEndOfGroupShwapPb = fmt.Errorf("proto: unexpected end of group")
)
