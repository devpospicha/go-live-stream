package rtmp

import (
	"encoding/binary"
	"encoding/hex"
	"errors"
	"net"
	"time"

	bin "github.com/devpospicha/go-live-stream/binary"
	"github.com/devpospicha/go-live-stream/rtmp/amf"
	"github.com/devpospicha/go-live-stream/rtmp/flv"
	log "github.com/sirupsen/logrus"
)

// User Control Message Events
const (
	ucmEventStreamBegin uint16 = iota
	ucmEventStreamEOF
	ucmEventStreamDry
	ucmEventSetBufferLength
	ucmEventStreamIsRecorded
	_
	ucmEventPingRequest
	ucmEventPingResponse
)

// NetConnection Commmands
const (
	cmdConnect      = "connect"
	cmdCall         = "call"
	cmdCreateStream = "createStream"
)

// NetStream Commands
const (
	cmdPlay            = "play"
	cmdPlay2           = "play2"
	cmdDeleteStream    = "deleteStream"
	cmdCloseStream     = "closeStream"
	cmdReceiveAudio    = "receiveAudio"
	cmdReceiveVideo    = "receiveVideo"
	cmdPublish         = "publish"
	cmdSeek            = "seek"
	cmdPause           = "pause"
	cmdReleaseStream   = "releaseStream"
	cmdGetStreamLength = "getStreamLength"
	cmdFCPublish       = "FCPublish"
	cmdFCUnpublish     = "FCUnpublish"
)

const packetBufLen = 1024 // Hold 1024 packets at most

type ConnInfo struct {
	app         string
	flashVer    string
	swfURL      string
	tcURL       string
	fpad        bool
	audioCodec  float64
	videoCodec  float64
	videoFunc   float64
	pageURL     string
	amfEncoding float64
}

type PublishOrPlayInfo struct {
	Name string
	Type string
}

type Conn struct {
	net.Conn
	chunkSize        uint32 // Size of chunk sent from server (Server -> chunk -> Client)
	clientChunkSize  uint32 // Size of chunk received from client (Server <- chunk <- Client)
	windowAckSize    uint32 // Window acknowledgement size of server
	clientWindowSize uint32 // Window acknowledgement size of client
	bandwidth        uint32 // Bandwidth of server
	clientBandwidth  uint32 // Bandwidth of client
	transactionID    float64
	ConnInfo
	info      *PublishOrPlayInfo
	StreamIDs []float64 // ID of currently allocated streams
	// Stream ID is increased only and the largest Stream ID
	// is placed at the end of slice
	closed         bool
	isPublisher    bool
	newStreamer    chan *Conn
	newViewer      chan *Conn
	channelCreated chan bool    // Get notfiy when stream channel has been created by server successfully
	channel        *Channel     // Streaming channel
	broadcast      chan *Packet // Channel to deliver streaming video and audio packets
	player         chan *Chunk  // Channel to receive decoded streaming video and audio chunks
	quit           chan bool    // Quit notify channel
}

func NewConn(c net.Conn, newStreamer chan *Conn, newViewer chan *Conn) *Conn {
	return &Conn{
		Conn:             c,
		chunkSize:        128,
		clientChunkSize:  128,
		windowAckSize:    2500000,
		clientWindowSize: 2500000,
		bandwidth:        2500000,
		clientBandwidth:  2500000,
		ConnInfo:         ConnInfo{amfEncoding: amf.AMF0},
		StreamIDs:        []float64{0}, // Default message stream for controls and commands with Message Stream ID: 0
		closed:           false,
		newStreamer:      newStreamer,
		newViewer:        newViewer,
		channelCreated:   make(chan bool),
		broadcast:        make(chan *Packet, packetBufLen),
		player:           make(chan *Chunk, 25),
		quit:             make(chan bool),
	}
}

func (c *Conn) Serve() {
	// Each connection has a default chunk stream
	cs := NewChunkStream(c)

	defer func() {
		// Quit underlying go routines
		select {
		case c.quit <- true:
			break
		}
	}()

	for {
		err := cs.readChunk()
		if err != nil {
			c.closed = true
		}

		// Check if the connection has been closed
		if c.closed {
			break
		}
	}
}

func (c *Conn) handleUsrCtrlMsg(cs *ChunkStream, chunk *Chunk) error {
	buf, err := cs.readBytes(chunk.Length)
	if err != nil {
		log.WithField("err", err).Error("Error while reading user control message.")
		return err
	}

	eventType := bin.U16BE(buf[:2])

	// TODO: Handle message
	//eventDataLength := cs.curRead.Length - 2
	//eventData := buf[2:]

	switch eventType {
	case ucmEventSetBufferLength:
		break
	case ucmEventPingResponse:
		break
	default:
		log.WithField("eventType", eventType).Warning("Unsupported event type.")
		return errors.New("Unsupported event type")
	}

	return nil
}

func (c *Conn) handleCmdMsg(cs *ChunkStream, chunk *Chunk) error {
	buf, err := cs.readAMFBody(chunk.Length, c.clientChunkSize)
	if err != nil {
		log.WithField("err", err).Error("Error while reading command message.")
		return err
	}

	amfDecoded, err := amf.DecodeAMF(buf, c.amfEncoding)
	if err != nil {
		log.WithField("err", err).Error("Error while reading command message.")
		return err
	}

	cmd := amfDecoded[0]

	switch cmd.(type) {
	case string:
		switch cmd.(string) {
		// NetConection commands
		case cmdConnect:
			err = c.connect(amfDecoded[1:])
			if err != nil {
				return err
			}

			return c.connectResp(cs, chunk)
		case cmdCall:
		// TODO:
		case cmdCreateStream:
			err = c.createStream(amfDecoded[1:])
			if err != nil {
				return err
			}

			return c.createStreamResp(cs, chunk)
		// NetStream commands
		case cmdPlay:
			err = c.play(amfDecoded[1:])
			if err != nil {
				return err
			}

			err = c.playResp(cs, chunk)
			if err != nil {
				return err
			}

			// Start playing video
			go c.playVideo(cs)

			return nil
		case cmdPlay2:
			return c.play2(amfDecoded[1:])
		case cmdDeleteStream:
			return c.deleteStream(amfDecoded[1:])
		case cmdCloseStream:
			return c.closeStream(amfDecoded[1:])
		case cmdReceiveAudio:
			return c.receiveVideo(amfDecoded[1:])
		case cmdReceiveVideo:
			return c.receiveAudio(amfDecoded[1:])
		case cmdPublish:
			err = c.publish(amfDecoded[1:])
			if err != nil {
				return err
			}

			err = c.publishResp(cs, chunk)
			if err != nil {
				return err
			}

			// Start broadcasting video
			go c.broadcastVideo()

			return nil
		case cmdSeek:
			return c.seek(amfDecoded[1:])
		case cmdPause:
			return c.pause(amfDecoded[1:])
		case cmdReleaseStream:
			return c.releaseStream(amfDecoded[1:])
		case cmdGetStreamLength:
			return c.getStreamLength(amfDecoded[1:])
		case cmdFCPublish:
			return c.fcPublish(amfDecoded[1:])
		case cmdFCUnpublish:
			return c.fcUnpublish(amfDecoded[1:])
		default:
			log.WithField("cmd", cmd.(string)).Warning("Unsupported command.")
			return errors.New("Unsupported command")
		}
	}

	log.WithField("cmd", cmd.(string)).Warning("Unsupported command.")
	return errors.New("Unsupported command")
}

func (c *Conn) handleDataMsg(cs *ChunkStream, chunk *Chunk) error {
	buf, err := cs.readAMFBody(chunk.Length, c.clientChunkSize)
	if err != nil {
		log.WithField("err", err).Error("Error while reading command message.")
		return err
	}

	_, err = amf.DecodeAMF(buf, c.amfEncoding)
	if err != nil {
		log.WithField("err", err).Error("Error while reading command message.")
		return err
	}

	// TODO: Handle the decoded data message

	return nil
}

func (c *Conn) handleSharedObjectMsg(cs *ChunkStream) error {
	return nil
}

func (c *Conn) handleAudioMsg(cs *ChunkStream, chunk *Chunk) error {
	buf, err := cs.readAMFBody(chunk.Length, c.clientChunkSize)
	if err != nil {
		log.WithField("err", err).Error("Error while reading audio message.")
		return err
	}

	packet := NewPacket(typeAudio, chunk.Timestamp, chunk.StreamID, buf)

	// Log before sending to c.broadcast
	if c.info != nil {
		log.WithFields(log.Fields{
			"streamName": c.info.Name,
			"packetType": "audio",
			"timestamp":  chunk.Timestamp,
			"size":       len(buf),
		}).Debug("Publisher packet received, about to queue to c.broadcast.")
	} else {
		log.WithFields(log.Fields{
			"packetType": "audio",
			"timestamp":  chunk.Timestamp,
			"size":       len(buf),
		}).Debug("Anonymous publisher packet received, about to queue to c.broadcast.")
	}

	c.broadcast <- packet

	return nil
}

func (c *Conn) handleVideoMsg(cs *ChunkStream, chunk *Chunk) error {
	buf, err := cs.readAMFBody(chunk.Length, c.clientChunkSize)
	if err != nil {
		log.WithField("err", err).Error("Error while reading video message.")
		return err
	}

	// New H.264 specific logging for incoming publisher video data
	if len(buf) > 0 { // Need at least 1 byte for FrameType/CodecID
		frameType := (buf[0] & 0xf0) >> 4
		codecID := buf[0] & 0xf

		logFields := log.Fields{
			"timestamp":  chunk.Timestamp,
			"payloadLen": len(buf),
			"frameType":  frameType,
			"codecID":    codecID,
		}
		if c.info != nil && c.info.Name != "" {
			logFields["streamName"] = c.info.Name
		}

		if codecID == 7 { // H.264/AVC
			var avcPacketType byte = 0
			var compositionTime int32 = 0    // Actually uint24, represented as int32
			var relevantPayload []byte = nil // Payload after FLV video headers

			if len(buf) > 1 { // AVCPacketType is in buf[1]
				avcPacketType = buf[1]
				logFields["avcPacketType"] = avcPacketType

				if avcPacketType == 1 { // NALU: CodecID(1) + AVCPacketType(1) + CompositionTime(3) + Data(...)
					if len(buf) >= 5 {
						compositionTime = bin.I24BE(buf[2:5])
						if len(buf) > 5 {
							relevantPayload = buf[5:]
						} else {
							relevantPayload = []byte{} // Empty if no data after headers
						}
					} else { // Not enough bytes for CompositionTime + NALU data
						relevantPayload = buf[2:] // Or consider this an error/malformed
						log.WithFields(logFields).Warn("H.264 NALU packet too short for CompositionTime.")
					}
				} else { // AVCPacketType 0 (Seq Header) or 2 (EOS) or other: CodecID(1) + AVCPacketType(1) + Data(...)
					if len(buf) > 2 {
						relevantPayload = buf[2:]
					} else {
						relevantPayload = []byte{} // Empty if no data after headers
					}
					// CompositionTime is 0 for these AVCPacketTypes as per FLV spec
				}
			} else { // Only 1 byte in buf, cannot determine AVCPacketType
				relevantPayload = buf[1:] // Or consider this an error/malformed
				log.WithFields(logFields).Warn("H.264 packet too short for AVCPacketType.")
			}

			logFields["compositionTime"] = compositionTime

			previewLen := 10
			if relevantPayload != nil {
				if len(relevantPayload) < previewLen {
					previewLen = len(relevantPayload)
				}
				logFields["payloadHex"] = hex.EncodeToString(relevantPayload[:previewLen])
			} else {
				// If relevantPayload is nil (e.g. due to short packet), log first few bytes of buf
				if len(buf) < previewLen {
					previewLen = len(buf)
				}
				logFields["payloadHex"] = hex.EncodeToString(buf[:previewLen]) + " (short_packet_preview)"
			}
			log.WithFields(logFields).Debug("H.264 video data received from publisher.")

		} else { // Non-H.264 video
			previewLen := 10
			if len(buf) < previewLen {
				previewLen = len(buf)
			}
			logFields["payloadHex"] = hex.EncodeToString(buf[:previewLen])
			log.WithFields(logFields).Debug("Non-H.264 video data received from publisher.")
		}
	} else { // Empty buffer
		logFields := log.Fields{"timestamp": chunk.Timestamp, "payloadLen": len(buf)}
		if c.info != nil && c.info.Name != "" {
			logFields["streamName"] = c.info.Name
		}
		log.WithFields(logFields).Warn("Empty video packet received from publisher.")
	}

	packet := NewPacket(typeVideo, chunk.Timestamp, chunk.StreamID, buf)
	c.broadcast <- packet

	return nil
}

func (c *Conn) handleAggregateMsg() error {
	return nil
}

// Protocol Control Messages

func (c *Conn) setChunkSize() error {
	var chunkSize uint32
	err := binary.Read(c, binary.BigEndian, &chunkSize)
	if err != nil {
		log.WithField("err", err).Error("Error while handling set chunk size command.")
		return err
	}

	if chunkSize > 0xFFFFFF {
		chunkSize = 0xFFFFFF
	}

	c.clientChunkSize = chunkSize

	return nil
}

func (c *Conn) respChunkSize(cs *ChunkStream) error {
	buf := make([]byte, 4)
	bin.PutU32BE(buf, c.chunkSize)
	chunk := NewPCMChunk(typeIDSetChunkSize, buf)
	return cs.writeChunk(chunk, c.chunkSize)
}

func (c *Conn) setWindowAckSize() error {
	var windowAckSize uint32
	err := binary.Read(c, binary.BigEndian, &windowAckSize)
	if err != nil {
		log.WithField("err", err).Error("Error while handling set window ack size command.")
	}

	c.clientWindowSize = windowAckSize

	return nil
}

func (c *Conn) respWindowAckSize(cs *ChunkStream) error {
	buf := make([]byte, 4)
	bin.PutU32BE(buf, c.windowAckSize)
	chunk := NewPCMChunk(typeIDWindowAckSize, buf)
	return cs.writeChunk(chunk, c.chunkSize)
}

func (c *Conn) respBandwidth(cs *ChunkStream) error {
	buf := make([]byte, 5)
	bin.PutU32BE(buf[:5], c.bandwidth)
	buf[4] = 2 // Dynamic
	chunk := NewPCMChunk(typeIDSetPeerBandwidth, buf)
	return cs.writeChunk(chunk, c.chunkSize)
}

// End of Protocol Control Messages

// User Control Messages

func (c *Conn) streamBegin(cs *ChunkStream, chunk *Chunk) error {
	buf := make([]byte, 6)
	bin.PutU16BE(buf[:2], ucmEventStreamBegin)
	bin.PutU32BE(buf[2:], chunk.StreamID)
	ucmChunk := NewUCMChunk(buf)
	return cs.writeChunk(ucmChunk, c.chunkSize)
}

func (c *Conn) streamEOF(cs *ChunkStream) error {
	return nil
}

func (c *Conn) streamDry(cs *ChunkStream) error {
	return nil
}

func (c *Conn) streamIsRecorded(cs *ChunkStream) error {
	return nil
}

func (c *Conn) pingRequest(cs *ChunkStream) error {
	return nil
}

// End of User Control Messages

// NetConnection Commands

func (c *Conn) connect(packets []interface{}) error {
	for i, p := range packets {
		if i == 0 {
			// Transaction ID
			id := p.(float64)
			if id != 1 {
				return errors.New("connect command's Transaction ID is not 1")
			}

			c.transactionID = id
		} else if i == 1 {
			// Command Object
			obj := p.(amf.Object)

			if app, ok := obj["app"]; ok {
				c.app = app.(string)
			}

			if flashVer, ok := obj["flashVer"]; ok {
				c.flashVer = flashVer.(string)
			}

			if swURL, ok := obj["swfUrl"]; ok {
				c.swfURL = swURL.(string)
			}

			if tcURL, ok := obj["tcUrl"]; ok {
				c.tcURL = tcURL.(string)
			}

			if fpad, ok := obj["fpad"]; ok {
				c.fpad = fpad.(bool)
			}

			if audioCodec, ok := obj["audioCodec"]; ok {
				c.audioCodec = audioCodec.(float64)
			}

			if videoCodec, ok := obj["videoCodec"]; ok {
				c.videoCodec = videoCodec.(float64)
			}

			if videoFunc, ok := obj["videoFunction"]; ok {
				c.videoFunc = videoFunc.(float64)
			}

			if pageURL, ok := obj["pageUrl"]; ok {
				c.pageURL = pageURL.(string)
			}

			if objEncoding, ok := obj["objectEncoding"]; ok {
				c.amfEncoding = objEncoding.(float64)
			}
		}
	}

	return nil
}

func (c *Conn) connectResp(cs *ChunkStream, chunk *Chunk) error {
	err := c.respWindowAckSize(cs)
	if err != nil {
		return err
	}

	err = c.respBandwidth(cs)
	if err != nil {
		return err
	}

	// Set size of chunk sent from server to 1024
	c.chunkSize = 1024
	err = c.respChunkSize(cs)
	if err != nil {
		return nil
	}

	cmdName := "_result"
	var transactionID float64 = 1

	props := amf.Object{}
	props["fmsVer"] = "FMS/3,0,1,123"
	props["capabilities"] = 31

	info := amf.Object{}
	info["code"] = "NetConnection.Connect.Success"
	info["level"] = "status"
	info["description"] = "The connection attempt succeeded."
	info["objectEncoding"] = c.amfEncoding

	return c.cmdResp(cs, chunk, cmdName, transactionID, props, info)
}

func (c *Conn) createStream(packets []interface{}) error {
	// Increase largest Message Stream ID as allocating a new stream for client
	newStreamID := c.StreamIDs[len(c.StreamIDs)-1] + 1
	c.StreamIDs = append(c.StreamIDs, newStreamID)

	for i, p := range packets {
		if i == 0 {
			// Transaction ID
			c.transactionID = p.(float64)
		}

		// Ignore Command Object
		break
	}

	return nil
}

func (c *Conn) createStreamResp(cs *ChunkStream, chunk *Chunk) error {
	cmdName := "_result"
	transactionID := c.transactionID
	info := interface{}(nil)
	streamID := c.StreamIDs[len(c.StreamIDs)-1]
	return c.cmdResp(cs, chunk, cmdName, transactionID, info, streamID)
}

// End of NetConnection Commands

// NetStream Commands

func (c *Conn) play(packets []interface{}) error {
	var streamName string

	for i, p := range packets {
		if i == 0 {
			// Transaction ID
			c.transactionID = p.(float64)
		} else if i == 2 {
			// Stream name
			streamName = p.(string)
		}

		// Ignore Command Object
		// TODO: Handle Start, Duration, Reset
	}

	if c.info == nil {
		c.info = &PublishOrPlayInfo{}
	}

	c.info.Name = streamName
	c.isPublisher = false

	// Notify server there's a new streamer
	c.newViewer <- c

	// Wait for server to create channel
	<-c.channelCreated

	return nil
}

func (c *Conn) playResp(cs *ChunkStream, chunk *Chunk) error {
	//  UserControl (StreamBegin)
	err := c.streamBegin(cs, chunk)
	if err != nil {
		return nil
	}

	cmdName := "onStatus"
	var transactionID float64 // = 0
	cmdObj := interface{}(nil)

	// Command Message (onStatus-play reset)
	info := amf.Object{}
	info["code"] = "NetStream.Play.Reset"
	info["level"] = "status"
	info["description"] = "Caused by a play list reset."
	info["objectEncoding"] = c.amfEncoding

	err = c.cmdResp(cs, chunk, cmdName, transactionID, cmdObj, info)
	if err != nil {
		return nil
	}

	// Command Message (onStatus-play start)
	info = amf.Object{}
	info["code"] = "NetStream.Play.Start"
	info["level"] = "status"
	info["description"] = "Playback has started."
	info["objectEncoding"] = c.amfEncoding

	return c.cmdResp(cs, chunk, cmdName, transactionID, cmdObj, info)
}

func (c *Conn) play2(packets []interface{}) error {
	return nil
}

func (c *Conn) deleteStream(packets []interface{}) error {
	if len(packets) < 3 {
		log.Error("deleteStream: streamID to be deleted not assigned.")
		err := errors.New("deleteStream: streamID to be deleted not assigned")
		return err
	}

	streamID := packets[2].(float64)

	var index = -1

	for i := 0; i < len(c.StreamIDs); i++ {
		if c.StreamIDs[i] == streamID {
			index = i
			break
		}
	}

	if index == -1 {
		// streamID to be deleted not found, do nothing
		log.Warn("deleteStream: streamID to be deleted not exists.")
		return nil
	}

	c.StreamIDs = append(c.StreamIDs[:index], c.StreamIDs[index+1:]...)

	if len(c.StreamIDs) <= 1 {
		// Only the default message stream exists or all streams has been deleted,
		// close connection
		log.WithField("streamName", c.info.Name).Info("deleteStream.")
		c.closed = true
	}

	return nil
}

func (c *Conn) closeStream(packets []interface{}) error {
	return nil
}

func (c *Conn) receiveVideo(packets []interface{}) error {
	return nil
}

func (c *Conn) receiveAudio(packets []interface{}) error {
	return nil
}

func (c *Conn) publish(packets []interface{}) error {
	if c.info == nil {
		c.info = &PublishOrPlayInfo{}
	}

	for i, p := range packets {
		if i == 0 {
			// Transaction ID
			c.transactionID = p.(float64)
		} else if i == 2 {
			// Publishing Name
			c.info.Name = p.(string)
		} else if i == 3 {
			// Publishing Type
			c.info.Type = p.(string)
		}
	}

	c.isPublisher = true

	// Notify server there's a new streamer
	c.newStreamer <- c

	// Wait for server to create channel
	<-c.channelCreated

	log.WithFields(log.Fields{
		"name": c.info.Name,
		"type": c.info.Type,
	}).Info("Publisher connected.")

	return nil
}

func (c *Conn) publishResp(cs *ChunkStream, chunk *Chunk) error {
	cmdName := "onStatus"
	var transactionID float64 // = 0
	cmdObj := interface{}(nil)

	info := amf.Object{}
	info["code"] = "NetStream.Publish.Start"
	info["level"] = "status"
	info["description"] = "Publish was successful."
	info["objectEncoding"] = c.amfEncoding

	return c.cmdResp(cs, chunk, cmdName, transactionID, cmdObj, info)
}

func (c *Conn) seek(packets []interface{}) error {
	return nil
}

func (c *Conn) pause(packets []interface{}) error {
	return nil
}

func (c *Conn) releaseStream(packets []interface{}) error {
	// Do nothing
	return nil
}

func (c *Conn) getStreamLength(packets []interface{}) error {
	// Do nothing
	return nil
}

func (c *Conn) fcPublish(packets []interface{}) error {
	// Do nothing
	return nil
}

func (c *Conn) fcUnpublish(packets []interface{}) error {
	return nil
}

// End of NetStream Commands

func (c *Conn) cmdResp(cs *ChunkStream, chunk *Chunk, cmdName string, transactionID float64, elems ...interface{}) error {
	amfBody := []interface{}{cmdName, transactionID}
	amfBody = append(amfBody, elems...)
	amfBodyEncoded, err := amf.EncodeAMF(amfBody, c.amfEncoding)
	if err != nil {
		return err
	}

	amfCmdChunk := NewAMFCmdChunk(c.amfEncoding, chunk.CSID, chunk.StreamID, amfBodyEncoded)
	return cs.writeChunk(amfCmdChunk, c.chunkSize)
}

func (c *Conn) broadcastVideo() {
	for {
		if c.closed {
			break // Exit if connection is marked as closed.
		}

		// Get current viewer count safely
		var currentViewersCount int
		c.channel.lock.RLock()
		currentViewersCount = len(c.channel.viewers)
		c.channel.lock.RUnlock()

		// Logic for managing c.broadcast based on viewer count
		if len(c.broadcast) == cap(c.broadcast) && currentViewersCount == 0 {
			log.Warn("broadcastVideo: c.broadcast is full and no viewers. Dropping oldest packet from c.broadcast.")
			<-c.broadcast // Drop oldest packet to make space for publisher
			// Optional: Add a metric here for dropped packets from c.broadcast
			continue // Retry the loop to re-evaluate conditions
		}

		if len(c.broadcast) == 0 && currentViewersCount == 0 {
			// No packets to process and no one is watching.
			// Sleep briefly to avoid busy-looping if publisher is also idle.
			time.Sleep(10 * time.Millisecond) // Requires import "time"
			continue
		}

		// If there are viewers OR there's something in c.broadcast to process for potential future viewers
		// (even if currentViewersCount is 0 now, one might connect just as a packet arrives)
		// proceed to the select block.

		select {
		case packet := <-c.broadcast:
			// Log after packet is received from c.broadcast
			if c.info != nil {
				log.WithFields(log.Fields{
					"streamName": c.info.Name,
					"packetType": packet.packetType,
					"timestamp":  packet.timestamp,
					"size":       len(packet.data),
				}).Debug("Packet picked from c.broadcast for distribution.")
			} else {
				log.WithFields(log.Fields{
					"packetType": packet.packetType,
					"timestamp":  packet.timestamp,
					"size":       len(packet.data),
				}).Debug("Anonymous publisher packet picked from c.broadcast for distribution.")
			}

			chunk := packet.decode()
			if chunk != nil {
				// Critical section to get a consistent list of viewers
				c.channel.lock.RLock()
				currentViewers := make([]*Conn, len(c.channel.viewers))
				copy(currentViewers, c.channel.viewers)
				c.channel.lock.RUnlock() // Release lock as soon as copy is made

				// Iterate over the copied list
				for _, viewer := range currentViewers {
					// Ensure viewer connection is still active before attempting to send.
					// This check helps prevent panics if a viewer disconnects
					// after the viewer list was copied but before this point.
					// A simple check could be if viewer.closed is true, though
					// sending to a closed channel (viewer.player) is the primary concern.
					// The non-blocking send from the previous step already mitigates
					// sending to a full buffer which might be on a closing connection.
					// However, a more robust check for viewer liveness might be needed
					// if connections are abruptly closed. For now, rely on the non-blocking send.

					// Log before attempting to send to viewer.player
					log.WithFields(log.Fields{
						"streamName": viewer.info.Name, // Assuming viewer.info is not nil for viewers
						"viewerAddr": viewer.RemoteAddr().String(),
						"packetType": packet.packetType, // Using packet.packetType for consistency with pickup log
						"timestamp":  packet.timestamp,  // Using packet.timestamp for consistency
					}).Debug("Attempting to send chunk to viewer.")

					select {
					case viewer.player <- chunk:
						log.WithFields(log.Fields{
							"streamName": viewer.info.Name,
							"viewerAddr": viewer.RemoteAddr().String(),
							"packetType": packet.packetType,
							"timestamp":  packet.timestamp,
						}).Debug("Chunk successfully sent to viewer.")
					default:
						// Existing Warn log is good. Add a debug log if more detail for this case is needed.
						if viewer.info != nil {
							log.WithFields(log.Fields{
								"streamName": viewer.info.Name,
								"viewerAddr": viewer.RemoteAddr().String(),
								"packetType": packet.packetType,
								"timestamp":  packet.timestamp,
							}).Debug("Chunk dropped for viewer (already logged as Warn).")
						} else {
							log.WithFields(log.Fields{
								"viewerAddr": viewer.RemoteAddr().String(),
								"packetType": packet.packetType,
								"timestamp":  packet.timestamp,
							}).Debug("Chunk dropped for anonymous viewer (already logged as Warn).")
						}
						// The original Warn log:
						if viewer.info != nil {
							log.WithFields(log.Fields{
								"streamName": viewer.info.Name,
								"viewerAddr": viewer.RemoteAddr().String(),
							}).Warn("Dropping packet for viewer; player buffer full.")
						} else {
							log.WithField("viewerAddr", viewer.RemoteAddr().String()).Warn("Dropping packet for anonymous viewer; player buffer full.")
						}
					}
				}
			}
		case <-c.quit:
			log.WithField("streamName", c.info.Name).Info("broadcastVideo quit.")
			return // Exit the broadcastVideo goroutine
		}
	}
}

func (c *Conn) playVideo(cs *ChunkStream) {
	for {
		select {
		case chunk := <-c.player:
			// Double check if the connection has not been closed yet
			if c.closed {
				log.WithField("streamName", c.info.Name).Info("playVideo quit.")
				break
			}

			// Log after chunk is received from c.player
			if c.info != nil {
				log.WithFields(log.Fields{
					"streamName": c.info.Name,
					"viewerAddr": c.RemoteAddr().String(),
					"chunkType":  chunk.TypeID,
					"timestamp":  chunk.Timestamp,
					"size":       chunk.Length,
				}).Debug("Chunk received from player channel for sending to client.")
			} else {
				log.WithFields(log.Fields{
					"viewerAddr": c.RemoteAddr().String(),
					"chunkType":  chunk.TypeID,
					"timestamp":  chunk.Timestamp,
					"size":       chunk.Length,
				}).Debug("Chunk received from player channel for sending to anonymous client.")
			}

			err := cs.writeChunk(chunk, c.chunkSize)
			if err == nil {
				if c.info != nil {
					log.WithFields(log.Fields{
						"streamName": c.info.Name,
						"viewerAddr": c.RemoteAddr().String(),
						"chunkType":  chunk.TypeID,
						"timestamp":  chunk.Timestamp,
					}).Debug("Chunk successfully written to client network.")
				} else {
					log.WithFields(log.Fields{
						"viewerAddr": c.RemoteAddr().String(),
						"chunkType":  chunk.TypeID,
						"timestamp":  chunk.Timestamp,
					}).Debug("Chunk successfully written to anonymous client network.")
				}
			} else {
				if c.info != nil {
					log.WithFields(log.Fields{
						"streamName": c.info.Name,
						"viewerAddr": c.RemoteAddr().String(),
						"chunkType":  chunk.TypeID,
						"error":      err,
					}).Error("Failed to write chunk to client network.")
				} else {
					log.WithFields(log.Fields{
						"viewerAddr": c.RemoteAddr().String(),
						"chunkType":  chunk.TypeID,
						"error":      err,
					}).Error("Failed to write chunk to anonymous client network.")
				}
			}
		case <-c.quit:
			log.WithField("streamName", c.info.Name).Info("playVideo quit.")
			break
		}
	}
}

func (p *Packet) decode() (chunk *Chunk) {
	switch p.packetType {
	case typeAudio:
		audio := flv.DecodeAudio(p.data)
		data := append(audio.AudioTagHeader.Encode(), audio.Data...)
		chunk = NewAudioChunk(p.timestamp, p.streamID, data)
		log.WithFields(log.Fields{
			"length":    len(data),
			"timestamp": p.timestamp,
		}).Debug("Decoded audio packet.")
	case typeVideo:
		video := flv.DecodeVideo(p.data) // video is *flv.VideoBody

		if video.VideoTagHeader.CodecID == 7 { // Assuming 7 is AVC/H.264
			payloadPreviewLen := 10
			if len(video.Data) < payloadPreviewLen {
				payloadPreviewLen = len(video.Data)
			}
			log.WithFields(log.Fields{
				"timestamp":       p.timestamp,
				"streamID":        p.streamID,
				"codecID":         video.VideoTagHeader.CodecID,
				"frameType":       video.VideoTagHeader.FrameType,     // 1: keyframe, 2: inter frame
				"avcPacketType":   video.VideoTagHeader.AVCPacketType, // 0: sequence header, 1: NALU, 2: EOS
				"compositionTime": video.VideoTagHeader.CompositionTime,
				"payloadLen":      len(video.Data),
				"payloadHex":      hex.EncodeToString(video.Data[:payloadPreviewLen]),
			}).Debug("H.264 video packet details (after FLV decode, before chunking).")
		}

		data := append(video.VideoTagHeader.Encode(), video.Data...)
		chunk = NewVideoChunk(p.timestamp, p.streamID, data)

		if video.VideoTagHeader.CodecID != 7 {
			log.WithFields(log.Fields{
				"length":    len(data), // This is re-encoded data length
				"timestamp": p.timestamp,
			}).Debug("Decoded non-H.264 video packet.")
		}
	default:
		log.WithField("type", p.packetType).Error("Cannot play unknown type packet")
	}

	return
}
