// Copyright 2018, OpenCensus Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tracetranslator

import (
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"reflect"
	"strconv"
	"time"

	"github.com/census-instrumentation/opencensus-service/internal"

	commonpb "github.com/census-instrumentation/opencensus-proto/gen-go/agent/common/v1"
	agenttracepb "github.com/census-instrumentation/opencensus-proto/gen-go/agent/trace/v1"
	tracepb "github.com/census-instrumentation/opencensus-proto/gen-go/trace/v1"
	"github.com/jaegertracing/jaeger/thrift-gen/jaeger"
)

// JaegerThriftBatchToOCProto converts a single Jaeger Thrift batch of spans to a OC proto batch.
func JaegerThriftBatchToOCProto(jbatch *jaeger.Batch) (*agenttracepb.ExportTraceServiceRequest, error) {
	ocbatch := &agenttracepb.ExportTraceServiceRequest{
		Node:  jProcessToOCProtoNode(jbatch.GetProcess()),
		Spans: jSpansToOCProtoSpans(jbatch.GetSpans()),
	}

	return ocbatch, nil
}

func jProcessToOCProtoNode(p *jaeger.Process) *commonpb.Node {
	if p == nil {
		return nil
	}

	node := &commonpb.Node{
		Identifier:  &commonpb.ProcessIdentifier{},
		LibraryInfo: &commonpb.LibraryInfo{},
		ServiceInfo: &commonpb.ServiceInfo{Name: p.GetServiceName()},
	}

	pTags := p.GetTags()
	attribs := make(map[string]string)

	for _, tag := range pTags {
		// Special treatment for special keys in the tags.
		switch tag.GetKey() {
		case "hostname":
			node.Identifier.HostName = tag.GetVStr()
			continue
		case "jaeger.version":
			node.LibraryInfo.ExporterVersion = "Jaeger-" + tag.GetVStr()
			continue
		}

		switch tag.GetVType() {
		case jaeger.TagType_STRING:
			attribs[tag.Key] = tag.GetVStr()
		case jaeger.TagType_DOUBLE:
			attribs[tag.Key] = strconv.FormatFloat(tag.GetVDouble(), 'f', -1, 64)
		case jaeger.TagType_BOOL:
			attribs[tag.Key] = strconv.FormatBool(tag.GetVBool())
		case jaeger.TagType_LONG:
			attribs[tag.Key] = strconv.FormatInt(tag.GetVLong(), 10)
		case jaeger.TagType_BINARY:
			attribs[tag.Key] = base64.StdEncoding.EncodeToString(tag.GetVBinary())
		default:
			attribs[tag.Key] = fmt.Sprintf("<Unknown Jaeger TagType %q>", tag.GetVType())
		}
	}

	if len(attribs) > 0 {
		node.Attributes = attribs
	}
	return node
}

var blankJaegerSpan = new(jaeger.Span)

func strToTruncatableString(s string) *tracepb.TruncatableString {
	if s == "" {
		return nil
	}
	return &tracepb.TruncatableString{Value: s}
}

func jSpansToOCProtoSpans(jspans []*jaeger.Span) []*tracepb.Span {
	spans := make([]*tracepb.Span, 0, len(jspans))
	for _, jspan := range jspans {
		if jspan == nil || reflect.DeepEqual(jspan, blankJaegerSpan) {
			continue
		}

		startTime := epochMicrosecondsAsTime(uint64(jspan.StartTime))
		_, sKind, sStatus, sAttributes := jtagsToAttributes(jspan.Tags)
		span := &tracepb.Span{
			TraceId: jTraceIDToOCProtoTraceID(jspan.TraceIdHigh, jspan.TraceIdLow),
			SpanId:  jSpanIDToOCProtoSpanID(jspan.SpanId),
			// TODO: Tracestate: Check RFC status and if is applicable,
			ParentSpanId: jSpanIDToOCProtoSpanID(jspan.ParentSpanId),
			Name:         strToTruncatableString(jspan.OperationName),
			Kind:         sKind,
			StartTime:    internal.TimeToTimestamp(startTime),
			EndTime:      internal.TimeToTimestamp(startTime.Add(time.Duration(jspan.Duration) * time.Microsecond)),
			Attributes:   sAttributes,
			// TODO: StackTrace: OpenTracing defines a semantic key for "stack", should we attempt to its content to StackTrace?
			TimeEvents: jLogsToOCProtoTimeEvents(jspan.Logs),
			Links:      jReferencesToOCProtoLinks(jspan.References),
			Status:     sStatus,
		}

		spans = append(spans, span)
	}
	return spans
}

func jLogsToOCProtoTimeEvents(logs []*jaeger.Log) *tracepb.Span_TimeEvents {
	if logs == nil {
		return nil
	}

	timeEvents := make([]*tracepb.Span_TimeEvent, 0, len(logs))

	for _, log := range logs {
		description, _, _, attribs := jtagsToAttributes(log.Fields)
		var annotation *tracepb.Span_TimeEvent_Annotation
		if attribs != nil {
			annotation = &tracepb.Span_TimeEvent_Annotation{
				Description: strToTruncatableString(description),
				Attributes:  attribs,
			}
		}
		timeEvent := &tracepb.Span_TimeEvent{
			Time:  internal.TimeToTimestamp(epochMicrosecondsAsTime(uint64(log.Timestamp))),
			Value: &tracepb.Span_TimeEvent_Annotation_{Annotation: annotation},
		}

		timeEvents = append(timeEvents, timeEvent)
	}

	return &tracepb.Span_TimeEvents{TimeEvent: timeEvents}
}

func jReferencesToOCProtoLinks(jrefs []*jaeger.SpanRef) *tracepb.Span_Links {
	if jrefs == nil {
		return nil
	}

	links := make([]*tracepb.Span_Link, 0, len(jrefs))

	for _, jref := range jrefs {
		var linkType tracepb.Span_Link_Type
		if jref.RefType == jaeger.SpanRefType_CHILD_OF {
			linkType = tracepb.Span_Link_CHILD_LINKED_SPAN
		} else {
			// TODO: SpanRefType_FOLLOWS_FROM doesn't map well to OC, so treat all other cases as unknown
			linkType = tracepb.Span_Link_TYPE_UNSPECIFIED
		}

		link := &tracepb.Span_Link{
			TraceId: jTraceIDToOCProtoTraceID(jref.TraceIdHigh, jref.TraceIdLow),
			SpanId:  jSpanIDToOCProtoSpanID(jref.SpanId),
			Type:    linkType,
		}
		links = append(links, link)
	}

	return &tracepb.Span_Links{Link: links}
}

func jTraceIDToOCProtoTraceID(high, low int64) []byte {
	if high == 0 && low == 0 {
		return nil
	}
	traceID := make([]byte, 16)
	binary.BigEndian.PutUint64(traceID[:8], uint64(high))
	binary.BigEndian.PutUint64(traceID[8:], uint64(low))
	return traceID
}

func jSpanIDToOCProtoSpanID(id int64) []byte {
	if id == 0 {
		return nil
	}
	spanID := make([]byte, 8)
	binary.BigEndian.PutUint64(spanID, uint64(id))
	return spanID
}

func jtagsToAttributes(tags []*jaeger.Tag) (string, tracepb.Span_SpanKind, *tracepb.Status, *tracepb.Span_Attributes) {
	if tags == nil {
		return "", tracepb.Span_SPAN_KIND_UNSPECIFIED, nil, nil
	}

	var sKind tracepb.Span_SpanKind

	var statusCodePtr *int32
	var statusMessage string
	var message string

	sAttribs := make(map[string]*tracepb.AttributeValue)

	for _, tag := range tags {
		// Take the opportunity to get the "span.kind" per OpenTracing spec, however, keep it also on the attributes.
		// TODO: (@pjanotti): Replace any OpenTracing literals by importing github.com/opentracing/opentracing-go/ext?
		switch tag.Key {
		case "span.kind":
			switch tag.GetVStr() {
			case "client":
				sKind = tracepb.Span_CLIENT
			case "server":
				sKind = tracepb.Span_SERVER
			}

		case "http.status_code", "status.code": // It is expected to be an int
			statusCodePtr = new(int32)
			*statusCodePtr = int32(tag.GetVLong())
			continue

		case "http.status_message", "status.message":
			statusMessage = tag.GetVStr()
			continue

		case "message":
			message = tag.GetVStr()
			continue
		}

		attrib := &tracepb.AttributeValue{}
		switch tag.GetVType() {
		case jaeger.TagType_STRING:
			attrib.Value = &tracepb.AttributeValue_StringValue{
				StringValue: &tracepb.TruncatableString{Value: tag.GetVStr()},
			}
		case jaeger.TagType_DOUBLE:
			attrib.Value = &tracepb.AttributeValue_DoubleValue{
				DoubleValue: tag.GetVDouble(),
			}
		case jaeger.TagType_BOOL:
			attrib.Value = &tracepb.AttributeValue_BoolValue{
				BoolValue: tag.GetVBool(),
			}
		case jaeger.TagType_LONG:
			attrib.Value = &tracepb.AttributeValue_IntValue{
				IntValue: tag.GetVLong(),
			}
		case jaeger.TagType_BINARY:
			attrib.Value = &tracepb.AttributeValue_StringValue{
				StringValue: &tracepb.TruncatableString{Value: base64.StdEncoding.EncodeToString(tag.GetVBinary())},
			}
		default:
			attrib.Value = &tracepb.AttributeValue_StringValue{
				StringValue: &tracepb.TruncatableString{Value: fmt.Sprintf("<Unknown Jaeger TagType %q>", tag.GetVType())},
			}
		}

		sAttribs[tag.Key] = attrib
	}

	var sStatus *tracepb.Status
	if statusCodePtr != nil || statusMessage != "" {
		statusCode := int32(0)
		if statusCodePtr != nil {
			statusCode = *statusCodePtr
		}
		sStatus = &tracepb.Status{Message: statusMessage, Code: statusCode}
	}

	var sAttributes *tracepb.Span_Attributes
	if len(sAttribs) > 0 {
		sAttributes = &tracepb.Span_Attributes{AttributeMap: sAttribs}
	}
	return message, sKind, sStatus, sAttributes
}

// epochMicrosecondsAsTime converts microseconds since epoch to time.Time value.
func epochMicrosecondsAsTime(ts uint64) (t time.Time) {
	if ts == 0 {
		return
	}
	return time.Unix(0, int64(ts*1e3)).UTC()
}
