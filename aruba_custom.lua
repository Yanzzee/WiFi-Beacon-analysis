-- Aruba ERM Wrapper Dissector (Reinjects inner frame as 802.11)
-- Install in ~/.config/wireshark/plugins/ or /usr/lib/wireshark/plugins/<version>/

local aruba_proto = Proto("aruba_custom", "Aruba Custom ERM Wrapper")

-- Optional: timestamp field for reference
local f_time = ProtoField.absolute_time("aruba_custom.time", "Aruba ERM Timestamp")
aruba_proto.fields = { f_time }

-- Forward reference to 802.11 dissector
local wlan_dissector = Dissector.get("wlan")

function aruba_proto.dissector(buffer, pinfo, tree)
    pinfo.cols.protocol = "ArubaERM"
    local subtree = tree:add(aruba_proto, buffer(), "Aruba ERM Encapsulated Frame")

    -- Optional: decode and show timestamp if present in first 8 bytes
    if buffer:len() >= 8 then
        local ts_bytes = buffer(0, 8):uint64():tonumber()
        local ts_sec = math.floor(ts_bytes / 1e6)
        local ts_usec = ts_bytes % 1e6
        if ts_sec > 1577836800 and ts_sec < 4102444800 then -- roughly 2020–2100
            subtree:add(f_time, NSTime(ts_sec, ts_usec))
        end
    end

    -- Reinject the rest of the payload (after timestamp) to the 802.11 dissector
    if buffer:len() > 8 then
        local inner_tvb = buffer(8):tvb()
        local inner_tree = subtree:add(buffer(8), "802.11 Payload")
        wlan_dissector:call(inner_tvb, pinfo, inner_tree)
    end
end

local udp_table = DissectorTable.get("udp.port")
udp_table:add(5555, aruba_proto)

function aruba_proto.init()
end