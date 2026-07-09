-- Aruba Custom UDP 5555 Dissector for Wireshark
-- Place this file in ~/.config/wireshark/plugins/ or /usr/lib/wireshark/plugins/<version>/

local aruba_proto = Proto("aruba_custom", "Aruba Custom ERM")

-- Fields to register
local f_time = ProtoField.absolute_time("aruba_custom.time", "Aruba ERM Time")
local f_ta   = ProtoField.ether("aruba_custom.wlan.ta", "Transmitter Address")
local f_ap   = ProtoField.string("aruba_custom.wlan.vs.ap_name", "Aruba AP Name")
local f_ssid = ProtoField.string("aruba_custom.wlan.ssid", "SSID")
local f_scount = ProtoField.uint8("aruba_custom.wlan.qbss.scount", "Station Count")
local f_cu = ProtoField.uint8("aruba_custom.wlan.qbss.cu", "Channel Utilization")
local f_adc = ProtoField.uint16("aruba_custom.wlan.qbss.adc", "Available Admission Capacity")
local f_channel = ProtoField.uint8("aruba_custom.wlan.ds.current_channel", "Current Channel")

aruba_proto.fields = { f_time, f_ta, f_ap, f_ssid, f_scount, f_cu, f_adc, f_channel }

function aruba_proto.dissector(buffer, pinfo, tree)
    print("[ArubaERM] Dissector called. Buffer length: " .. buffer:len())

    pinfo.cols.protocol = "ArubaERM"
    local subtree = tree:add(aruba_proto, buffer(), "Aruba ERM Encapsulated Frame")

    local ssid_offset = 108
    local ap_offset = 368
    local ta_offset = 42
    local qbss_offset = 124
    local chan_offset = 127

    local function safe_add(label, fn)
        local ok, err = pcall(fn)
        if not ok then
            print("[ArubaERM] Error adding field (" .. label .. "): " .. tostring(err))
        end
    end

    safe_add("timestamp", function()
        if buffer:len() >= 8 then
            local ts_bytes = buffer(0, 8):uint64():tonumber()
            local ts_sec = math.floor(ts_bytes / 1e6)
            local ts_usec = ts_bytes % 1e6
            subtree:add(f_time, NSTime(ts_sec, ts_usec))
            print("[ArubaERM] Timestamp: " .. ts_sec .. "." .. ts_usec)
        end
    end)

    safe_add("TA", function()
        if buffer:len() >= ta_offset + 6 then
            subtree:add(f_ta, buffer(ta_offset, 6))
        end
    end)

    safe_add("SSID", function()
        if buffer:len() >= ssid_offset + 32 then
            subtree:add(f_ssid, buffer(ssid_offset, 32):stringz())
        end
    end)

    safe_add("AP Name", function()
        if buffer:len() >= ap_offset + 32 then
            subtree:add(f_ap, buffer(ap_offset, 32):stringz())
        end
    end)

    safe_add("QBSS", function()
        if buffer:len() >= qbss_offset + 4 then
            subtree:add(f_scount, buffer(qbss_offset, 1))
            subtree:add(f_cu, buffer(qbss_offset + 1, 1))
            subtree:add(f_adc, buffer(qbss_offset + 2, 2))
        end
    end)

    safe_add("Channel", function()
        if buffer:len() >= chan_offset + 1 then
            subtree:add(f_channel, buffer(chan_offset, 1))
        end
    end)
end

local udp_table = DissectorTable.get("udp.port")
udp_table:add(5555, aruba_proto)
register_postdissector(aruba_proto)

function aruba_proto.init()
    print("[ArubaERM] Aruba Custom dissector loaded and initialized.")
end