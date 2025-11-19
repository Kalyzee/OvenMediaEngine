#include "nack.h"

#include <base/ovlibrary/byte_io.h>

#include "rtcp_private.h"

bool NACK::Parse(const RtcpPacket& packet)
{
	const uint8_t* payload = packet.GetPayload();
	size_t payload_size = packet.GetPayloadSize();

	if (payload_size < static_cast<size_t>(8 /*SSRC * 2*/ + 4 /*FCI*/))
	{
		logtd("Payload is too small to parse NACK");
		return false;
	}

	SetSrcSsrc(ByteReader<uint32_t>::ReadBigEndian(&payload[0]));
	SetMediaSsrc(ByteReader<uint32_t>::ReadBigEndian(&payload[4]));

	size_t fci_count = (packet.GetPayloadSize() - 8) / 4;
	size_t offset = 8; /* ssrc * 2 */
	for (size_t i = 0; i < fci_count; i++)
	{
		auto pid = ByteReader<uint16_t>::ReadBigEndian(&payload[offset]);
		auto blp = ByteReader<uint16_t>::ReadBigEndian(&payload[offset + 2]);

		// convert to id
		_lost_ids.push_back(pid);
		pid++;

		for (uint16_t mask = blp; mask != 0; mask >>= 1, ++pid)
		{
			if (mask & 1)
			{
				_lost_ids.push_back(pid);
			}
		}

		offset += 4; /*fci size*/
	}

	return true;
}

// RtcpInfo must provide raw data
std::shared_ptr<ov::Data> NACK::GetData() const
{
	/*
  //   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  // 0 |                  SSRC of packet sender                        |
  //   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  // 4 |                  SSRC of media source                         |
  //   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  //   :            Feedback Control Information (FCI)                 :
  //   :   PID | BLP | PID | BLP | PID | BLP | ...                     :
  //   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  */

	if (GetLostIdCount() == 0)
	{
		return nullptr;
	}

	std::vector<uint16_t> lost = _lost_ids;
	std::sort(lost.begin(), lost.end());

	std::vector<std::pair<uint16_t, uint16_t>> nack_blocks;

	size_t i = 0;
	while (i < lost.size())
	{
		uint16_t pid = lost[i];
		uint16_t blp = 0;

		size_t j = i + 1;
		while (j < lost.size() && lost[j] - pid <= 16)
		{
			uint16_t shift = lost[j] - pid - 1;
			blp |= (1 << shift);
			j++;
		}

		nack_blocks.emplace_back(pid, blp);
		i = j;
	}

	const size_t fci_size = nack_blocks.size() * 4;

	auto nack_data = std::make_shared<ov::Data>();
	nack_data->SetLength(4 + 4 + fci_size);

	ov::ByteStream stream(nack_data.get());

	// Feedback
	stream.WriteBE32(_src_ssrc);
	stream.WriteBE32(_media_ssrc);

	// FCI
	for (const auto& [pid, blp] : nack_blocks)
	{
		stream.WriteBE16(pid);
		stream.WriteBE16(blp);
	}

	return nack_data;
}

void NACK::DebugPrint()
{
	ov::String ids;

	for (size_t i = 0; i < GetLostIdCount(); i++)
	{
		uint16_t id = GetLostId(i);
		ids.AppendFormat("%u/", id);
	}

	logtd("NACK >> %s", ids.CStr());
}