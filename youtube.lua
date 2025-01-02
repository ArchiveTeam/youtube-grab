local urlparse = require("socket.url")
local http = require("socket.http")
local https = require("ssl.https")
local cjson = require("cjson")
local utf8 = require("utf8")

local item_dir = os.getenv("item_dir")
local warc_file_base = os.getenv("warc_file_base")
local v1_items_s = os.getenv("v1_items")
local v2_items_s = os.getenv("v2_items")
local item_type = nil
local item_name = nil
local item_value = nil
local more = 1
local url_count = 0
local tries = 0
local downloaded = {}
local seen_200 = {}
local addedtolist = {}
local abortgrab = false
local killgrab = false

local discovered = {}
local discovered_self = {}
local outlinks = {}

local bad_items = {}
local allowed_urls = {}

local context = {}
local post_headers = nil
local current_referer = nil
local current_content = nil
local sorted_new = {}
local decrypted_ns = {}

local v1_items = {}
for s in string.gmatch(v1_items_s, "([^;]+)") do
  v1_items[s] = true
end
local v2_items = {}
for s in string.gmatch(v2_items_s, "([^;]+)") do
  v2_items[s] = true
end

io.stdout:setvbuf("no")
math.randomseed(os.time())

local video_pattern = "[0-9a-zA-Z%-_]+"
local channel_pattern = video_pattern
local playlist_pattern = video_pattern
local user_pattern = "[^\"'%s%?&/]+"
local c_pattern = user_pattern

if not urlparse or not http then
  io.stdout:write("socket not corrently installed.\n")
  io.stdout:flush()
  abortgrab = true
end

local ids = {}

--[[for ignore in io.open("ignore-list", "r"):lines() do
  downloaded[ignore] = true
end]]

kill_grab = function(item)
  io.stdout:write("Aborting crawling.\n")
  killgrab = true
end

queue_item = function(type_, value)
  if type_ == "c" or type_ == "u" then
    temp = ""
    for c in string.gmatch(value, "(.)") do
      local b = string.byte(c)
      if b < 48
        or (b > 57 and b < 65)
        or (b > 90 and b < 97)
        or b > 122 then
        c = string.format("%%%02X", b)
      end
      temp = temp .. c
    end
    value = temp
  end
  new_item = type_ .. ":" .. value
  if not discovered[new_item] then
    discovered[new_item] = true
  end
end

get_item = function(url)
  local match = string.match(url, "^https?://www%.youtube%.com/watch%?v=(" .. video_pattern .. ")$")
  local type_ = "v"
  if v2_items[match] then
    type_ = "v2"
  elseif v1_items[match] then
    type_ = "v1"
  end
  if match and type_ then
    return type_, match
  end
end

set_new_item = function(url)
  local type_, match = get_item(url)
  if match and not ids[match] then
    sorted_new = {}
    context = {}
    post_headers = nil
    current_content = nil
    abortgrab = false
    exitgrab = false
    ids[match] = true
    item_value = match
    item_type = type_
    item_name = type_ .. ":" .. match
    io.stdout:write("Archiving item " .. item_name .. ".\n")
    io.stdout:flush()
  end
end

abort_item = function(item)
  abortgrab = true
  if not item then
    item = item_name
  end
  if not bad_items[item] then
    io.stdout:write("Aborting item " .. item .. ".\n")
    io.stdout:flush()
    bad_items[item] = true
  end
  error("Aborting.")
end

read_file = function(file)
  if file then
    local f = assert(io.open(file))
    local data = f:read("*all")
    f:close()
    return data
  else
    return ""
  end
end

allowed = function(url, parenturl)
  --[[if string.match(urlparse.unescape(url), "[<>\\%*%$%^%[%],%(%){}]") then
    return false
  end]]

  if allowed_urls[url] then
    return true
  end

  local tested = {}
  for s in string.gmatch(url, "([^/]+)") do
    if not tested[s] then
      tested[s] = 0
    end
    if tested[s] == 6 then
      return false
    end
    tested[s] = tested[s] + 1
  end

  for s in string.gmatch(url, "(" .. video_pattern .. ")") do
    if ids[s] then
      return true
    end
  end

  if string.match(url, "^https?://www%.youtube%.com/comment_service_ajax")
    or string.match(url, "^https?://[^/]*googlevideo%.com/")
    or string.match(url, "^https?://[^/]*youtube.com/youtubei/v1/next")
    or string.match(url, "^https?://[^/]*ytimg%.com") then
    return true
  end

  return false
end

wget.callbacks.download_child_p = function(urlpos, parent, depth, start_url_parsed, iri, verdict, reason)
  return false
end

wget.callbacks.get_urls = function(file, url, is_css, iri)
  local urls = {}
  local html = nil

  downloaded[url] = true

  local function check(urla)
    local origurl = url
    local url = string.match(urla, "^([^#]+)")
    local url_ = string.match(url, "^(.-)%.?$")
    url_ = string.gsub(url_, "&amp;", "&")
    url_ = string.match(url_, "^(.-)%s*$")
    url_ = string.match(url_, "^(.-)%??$")
    url_ = string.match(url_, "^(.-)&?$")
    url_ = string.match(url_, "^(.-)/?$")
    if (downloaded[url_] ~= true and addedtolist[url_] ~= true)
      and allowed(url_, origurl) then
      table.insert(urls, { url=url_ })
      addedtolist[url_] = true
      addedtolist[url] = true
    end
  end

  local function checknewurl(newurl)
    if string.match(newurl, "\\[uU]002[fF]") then
      return checknewurl(string.gsub(newurl, "\\[uU]002[fF]", "/"))
    end
    if string.match(newurl, "^https?:////") then
      check(string.gsub(newurl, ":////", "://"))
    elseif string.match(newurl, "^https?://") then
      check(newurl)
    elseif string.match(newurl, "^https?:\\/\\?/") then
      check(string.gsub(newurl, "\\", ""))
    elseif string.match(newurl, "^\\/") then
      checknewurl(string.gsub(newurl, "\\", ""))
    elseif string.match(newurl, "^//") then
      check(urlparse.absolute(url, newurl))
    elseif string.match(newurl, "^/") then
      check(urlparse.absolute(url, newurl))
    elseif string.match(newurl, "^%.%./") then
      if string.match(url, "^https?://[^/]+/[^/]+/") then
        check(urlparse.absolute(url, newurl))
      else
        checknewurl(string.match(newurl, "^%.%.(/.+)$"))
      end
    elseif string.match(newurl, "^%./") then
      check(urlparse.absolute(url, newurl))
    end
  end

  local function checknewshorturl(newurl)
    if string.match(newurl, "^%?") then
      check(urlparse.absolute(url, newurl))
    elseif not (string.match(newurl, "^https?:\\?/\\?//?/?")
      or string.match(newurl, "^[/\\]")
      or string.match(newurl, "^%./")
      or string.match(newurl, "^[jJ]ava[sS]cript:")
      or string.match(newurl, "^[mM]ail[tT]o:")
      or string.match(newurl, "^vine:")
      or string.match(newurl, "^android%-app:")
      or string.match(newurl, "^ios%-app:")
      or string.match(newurl, "^%${")) then
      check(urlparse.absolute(url, newurl))
    end
  end

  local function execute_js(code, arg)
    local filename = item_dir .. "/temp_func.js"
    local file = io.open(filename, "w")
    file:write("func" .. code .. 'console.log(func("' .. arg .. '"));')
    file:close()
    local command = "node " .. filename
    local stream = io.popen(command)
    local output = stream:read("*a")
    stream:close()
    local result = string.match(output, "^([^%s]+)")
    assert(result ~= nil)
    return result
  end

  local function decrypt_n(n, code)
    --[[print("extracting n description function", n)
    local f_name = string.match(code, '%([0-9a-zA-Z%$]+%s*=%s*([0-9a-zA-Z%$]+)%([0-9a-zA-Z%$]+%)%s*,%s*[0-9a-zA-Z%$]+%.set%(%s*"n"')
    if not f_name then
      f_name = string.match(code, '%([0-9a-zA-Z%$]+%s*=%s*([0-9a-zA-Z%$]+)%([0-9a-zA-Z%$]+%)%s*,%s*[0-9a-zA-Z%$]+%.set%(%s*"n"')
    end
    if not f_name then
      f_name = string.match(code, ',%s*[0-9a-zA-Z%$]+%.set%(%s*"n"%s*,%s*[0-9a-zA-Z%$]+%s*%)%s*,%s*[0-9a-zA-Z%$]+%.length%s*||%s*([0-9a-zA-Z%$]+)%(""%)')
    end
    if not f_name then
      f_name = string.match(code, ',%s*[0-9a-zA-Z%$]+%.set%(%s*[0-9a-zA-Z%$]+%s*,%s*[0-9a-zA-Z%$]+%s*%)%s*,%s*[0-9a-zA-Z%$]+%.length%s*||%s*([0-9a-zA-Z%$]+)%(""%)')
    end
    print(" - name:", f_name)
    local f_code = string.match(code, f_name .. "(%s*=%s*function%s*%(a%)%s*{.-return%s+[0-9a-zA-Z%.]+%([0-9a-zA-Z%$]+%s*,%s*%(\"\",\"\"%)%)%s*};)")
    if not f_code then
      f_code = string.match(code, f_name .. "(%s*=%s*function%s*%(a%)%s*{.-return%s+[0-9a-zA-Z%.]+%(\"\"%)%s*};)")
    end]]
    --local f_code = string.match(string.reverse(code), '(;}%)""%(nioj%.[0-9a-zA-Z$]+%s+nruter%s*}%s*[0-9a-zA-Z$]+%s*%+%s*"[^"]+_tpecxe_decnahne".-{%s*%)%s*a%(noitcnuf%s*=%s*)[0-9a-zA-Z$]+')
    local f_code = string.match(string.reverse(code), '(;}%)""%(nioj%.[0-9a-zA-Z$]+%s+nruter%s*}%s*[0-9a-zA-Z$]+%s*%+%s*"_8w_[^"]+".-{%s*%)%s*[a-zA-Z]%(noitcnuf%s*=%s*)[0-9a-zA-Z$]+')
    f_code = string.reverse(f_code)
    f_code = string.gsub(f_code, 'if%(typeof [0-9a-zA-Z]+==="undefined"%)return [0-9a-zA-Z]+;', "")
    local new_n = execute_js(f_code, n)
    print("decrypted n " .. n .. " to " .. new_n)
    assert(n ~= new_n)
    return new_n
  end

  local function decrypt_sig(s, code)
    local f_name = nil
    for _, pattern in pairs({
      "m=([0-9a-zA-Z%$]+)%(decodeURIComponent%(h%.s%)%)",
      "%(c=([0-9a-zA-Z%$]+)%(decodeURIComponent%(c%)%)",
      "[A-Z]&&%([A-Z]=([a-zA-Z_]+)%(decodeURIComponent%([A-Z]%)%)"
    }) do
      f_name = string.match(code, pattern)
      if f_name then
        break
      end
    end
    local f_code = string.match(code, f_name .. "(=function%([^%)]+%){[^}]+};)")
    local varname = string.match(f_code, ";([a-zA-Z_]+)%.[a-zA-Z_]+%([a-zA-Z],[0-9]+%);")
    local f_var = string.match(code, "(var " .. varname .. "={[a-zA-Z_]+:function.-}};)")
    return execute_js(f_code .. f_var, s)
  end

  --[[local function interpret_javascript(key, code, f_name)
    print("reading decryption rules")
    print(" - name:", f_name)
    local f_content = string.match(code, f_name .. "=function%(a%){([^}]+)}")
    local f_rules_name = string.match(f_content, ";([0-9a-zA-Z]+)%.")
    print(" - rules name:", f_rules_name)
    local f_rules = string.match(code, f_rules_name .. "={(.-)};")
    local rules = {}
    for name, args, expression in string.gmatch(f_rules, "([0-9a-zA-Z]+):function%(([^%)]+)%){([^}]+)}") do
      print(" -- interpreting rule", expression, "under name", name, "with args", args)
      local index = string.match(expression, "a%.splice%(([0-9]+),b%)")
      if index then
        index = tonumber(index) + 1
        rules[name] = function(a, b)
          print(" -- splicing", b, "times at index", index)
          for i=1,b do
            table.remove(a, index)
          end
          return a
        end
      elseif expression == "a.reverse()" then
        rules[name] = function(a)
          print(" -- reversing")
          local new_a = {}
          for i=#a,1,-1 do
            new_a[#new_a+1] = a[i]
          end
          return new_a
        end
      elseif string.match(expression, "%[") then
        rules[name] = function(a, b)
          print(" -- switching chars at", 0, "and", b)
          local c = a[1]
          local i = b % #a + 1
          a[1] = a[i]
          a[i] = c
          return a
        end
      else
        error("Decryption rule is unknown.")
      end
    end
    print("decrypting signature")
    local a = {}
    for c in string.gmatch(key, ".") do
      table.insert(a, c)
    end
    for expression in string.gmatch(f_content, "([^;]+)") do
      print(" - expression", expression)
      if not string.match(expression, "[= ]") then
        local rule_name, arg = string.match(expression, "%.([0-9a-zA-Z]+)%(a,([0-9]+)%)")
        print(" -- start:", table.concat(a, ""))
        a = rules[rule_name](a, tonumber(arg))
        print(" -- end:", table.concat(a, ""))
      else
        print(" -- skipping")
      end
    end
    return table.concat(a, "")
  end]]

  local function check_list_length(l)
    local count = 0
    for _ in pairs(l) do
      count = count + 1
    end
    if count ~= 1 then
      error("List should have length 1.")
    end
  end

  local function encode_body(d)
    local s = ""
    for k, v in pairs(d) do
      if string.len(s) > 0 then
        s = s .. "&"
      end
      s = s .. k .. "=" .. urlparse.escape(v)
    end
    return s
  end

  local function queue_comments(continuation, itct, session_token, replies, without_next)
    local action = "action_get_comments"
    if replies then
      action = "action_get_comment_replies"
    end
    local next_param = "&type=next"
    if without_next then
      next_param = ""
    end
    post_headers["Content-Type"] = "application/x-www-form-urlencoded"
    table.insert(
      urls,
      {
        url=
          "https://www.youtube.com/comment_service_ajax"
          .. "?" .. action .. "=1"
          .. "&pbj=1"
          .. "&ctoken=" .. urlparse.escape(continuation)
          .. "&continuation=" .. urlparse.escape(continuation)
          .. next_param
          .. "&itct=" .. urlparse.escape(itct),
        method="POST",
        body_data=encode_body({
          session_token=session_token
        }),
        headers=post_headers
      }
    )
  end

  local function set_current_context(context)
    context["client"]["screenWidthPoints"] = 1920
    context["client"]["screenHeightPoints"] = 1080
    context["client"]["screenPixelDensity"] = 1
    context["client"]["screenDensityFloat"] = 1
    context["client"]["utcOffsetMinutes"] = 0
    context["client"]["userInterfaceTheme"] = "USER_INTERFACE_THEME_LIGHT"
    context["client"]["utcOffsetMinutes"] = 0
    context["client"]["mainAppWebInfo"] = {
      graftUrl=current_referer,
      webDisplayMode="WEB_DISPLAY_MODE_BROWSER",
      isWebNativeShareAvailable=false
    }
    context["request"]["internalExperimentFlags"] = {}
    context["request"]["consistencyTokenJars"] = {}
    --context["clickTracking"]["clickTrackingParams"]
    context["adSignalsInfo"] = {
      params={
        {
          key="dt",
          value=tostring(os.time(os.date("!*t"))) .. string.format("%03d", math.random(100))
        }, {
          key="flash",
          value="0"
        }, {
          key="frm",
          value="0"
        }, {
          key="u_tz",
          value="0"
        }, {
          key="u_his",
          value="4"
        }, {
          key="u_java",
          value="false"
        }, {
          key="u_h",
          value="1080"
        }, {
          key="u_w",
          value="1920"
        }, {
          key="u_ah",
          value="1040"
        }, {
          key="u_aw",
          value="1920"
        }, {
          key="u_cd",
          value="24"
        }, {
          key="u_nplug",
          value="0"
        }, {
          key="u_nmime",
          value="0"
        }, {
          key="bc",
          value="31"
        }, {
          key="bih",
          value="1080"
        }, {
          key="biw",
          value="1903"
        }, {
          key="brdim",
          value="-8,-8,-8,-8,1920,0,1936,1056,1920,1080"
        }, {
          key="vis",
          value="1"
        }, {
          key="wgl",
          value="true"
        }, {
          key="ca_type",
          value="image"
        }
      }
    }
    current_context = context
  end

  local function next_endpoint(continuation, click_tracking_params, api_url, pretty_print)
    current_context["clickTracking"]["clickTrackingParams"] = click_tracking_params
    post_headers["Content-Type"] = "application/json"
    local pretty_print_s = nil
    if pretty_print == "both" then
      pretty_print = {"", "&prettyPrint=false"}
    elseif pretty_print == "no" then
      pretty_print = {""}
    elseif pretty_print == "yes" then
      pretty_print = {"&prettyPrint=false"}
    end
    for _, s in pairs(pretty_print) do
      table.insert(
        urls,
        {
          url="https://www.youtube.com" .. api_url .. "?key=" .. context["api_key"] .. s,
          method="POST",
          body_data=cjson.encode({
            context=current_context,
            continuation=continuation
          }),
          headers=post_headers
        }
      )
    end
  end

  local function queue_continuation_old(data, replies, without_next)
    local key = "nextContinuationData"
    if without_next then
      key = "reloadContinuationData"
    end
    if not data[key] then
      check_list_length(data)
      data = data[1]
    end
    data = data[key]
    queue_comments(
      data["continuation"],
      data["clickTrackingParams"],
      xsrf_token,
      replies,
      without_next
    )
  end

  local function queue_continuation_new(continuation_item_renderer, pretty_print)
    local continuation_endpoint = continuation_item_renderer["continuationEndpoint"]
    if not continuation_endpoint and not sorted_new[pretty_print] then
      print("switching order")
      continuation_endpoint = continuation_item_renderer
    end
    if not continuation_endpoint then
      print("getting more replies")
      continuation_endpoint = continuation_item_renderer["button"]["buttonRenderer"]["command"]
    end
    next_endpoint(
      continuation_endpoint["continuationCommand"]["token"],
      continuation_endpoint["clickTrackingParams"],
      continuation_endpoint["commandMetadata"]["webCommandMetadata"]["apiUrl"],
      pretty_print
    )
  end

  local function find_external_video_id(data)
    for k, v in pairs(data) do
      if type(v) == "table" then
        find_external_video_id(v)
      elseif type(k) == "string" and k == "externalVideoId" then
        discovered_self["v2:" .. v] = true
      end
    end
  end

  local function queue_streams(initial_player)
    local current_diff = nil
    local current_height = nil
    local current_fps = nil
    local current_video_codec = nil
    local current_video_url = nil
    local current_audio_url = {}
    local current_audio_default = nil
    for _, format in pairs(initial_player["streamingData"]["adaptiveFormats"]) do
      local mime = format["mimeType"]
      if string.match(mime, "^video/") then
        local height = format["height"]
        local fps = format["fps"]
        local codec = string.match(mime, "codecs=\"([0-9a-zA-Z]+)")
        print("Checking video with fps " .. fps .. ", height " .. height .. ", codec " .. codec)
        local diff = math.abs(height-480)
        if not current_video_url
          or (item_type == "v1" and diff < current_diff and not context["180"] and not context["360"])
          or (
            (context["180"] or context["360"] or item_type == "v2")
            and (
              height > current_height
              or (
                height >= current_height
                and (
                  fps > current_fps
                  or (current_video_codec ~= "vp9" and codec == "vp9")
                )
              )
            )
          ) then
          current_diff = diff
          current_fps = fps
          current_video_codec = codec
          current_height = height
          current_video_url = {url=format["url"], cipher=format["signatureCipher"]}
        end
      elseif string.match(mime, "^audio/") then
        local bitrate = format["bitrate"]
        local codec = string.match(mime, "codecs=\"([0-9a-zA-Z]+)")
        local drc = false
        local name = ""
        if format["isDrc"] then
          drc = true
        end
        if format["audioTrack"] and format["audioTrack"]["displayName"] then
          name = format["audioTrack"]["displayName"]
          if format["audioTrack"]["audioIsDefault"] then
            if current_audio_default ~= nil and current_audio_default ~= name then
              error("More than two default audio streams?")
            end
            current_audio_default = name
          end
        end
        local name_string = name
        if string.len(name_string) > 0 then
          name_string = ' \'' .. name_string .. '\''
        end
        print("Checking audio" .. name .. " with bitrate " .. bitrate .. ", DRC " .. tostring(drc) .. ", codec " .. codec)
        if not current_audio_url[name]
          or (not drc and current_audio_url[name]["drc"])
          or (
            (not drc or current_audio_url[name]["drc"])
            and (
              bitrate > current_audio_url[name]["bitrate"]
              --or (codec == "opus" and current_audio_url[name]["codec"] ~= "opus")
            )
          ) then
          current_audio_url[name] = {
            ["url"] = format["url"],
            ["cipher"] = format["signatureCipher"],
            ["bitrate"] = bitrate,
            ["drc"] = drc,
            ["codec"] = codec
          }
        end
      else
        error("Unknown media... please report on IRC or archiveteam@archiveteam.org!")
      end
    end

    print("Chosen video with fps " .. current_fps .. ", height " .. current_height .. ", codec " .. current_video_codec)
    for audio_name, audio_data in pairs(current_audio_url) do
      local name_string = audio_name
      if string.len(name_string) > 0 then
        name_string = ' \'' .. name_string .. '\''
      end
      print("Chosen audio" .. name_string .. " with bitrate " .. audio_data["bitrate"] .. ", DRC " .. tostring(audio_data["drc"]) .. ", codec " .. audio_data["codec"])
    end

    local player_js_url = urlparse.absolute("https://www.youtube.com/", context["ytplayer"]["PLAYER_JS_URL"])
    print(" - using PLAYER_JS_URL", player_js_url)
    local body, _, _, _ = https.request(player_js_url)
    if math.random() < 0.05 then
      check(player_js_url)
    end

    local streams = {
      ["video"]=current_video_url,
    }
    audio_stream_count = 0
    for audio_name, d in pairs(current_audio_url) do
      audio_stream_count = audio_stream_count + 1
      streams["audio " .. audio_name] = d
    end

    for stream_type, stream_data in pairs(streams) do
      if stream_data["url"] == nil then
        print("found encrypted signature")
        local signature_cipher = stream_data["cipher"]
        --print(" - signature cipher", signature_cipher)
        local s = urlparse.unescape(string.match(signature_cipher, "^s=([^&]+)"))
        local sp = urlparse.unescape(string.match(signature_cipher, "&sp=([^&]+)"))
        local url_ = urlparse.unescape(string.match(signature_cipher, "&url=([^&]+)"))
        local s_decrypted = decrypt_sig(s, body)
        stream_data["url"] = url_ .. "&" .. sp .. "=" .. s_decrypted
        --print(" - decrypted signature to", stream_data["url"])
      end
      local newurl = stream_data["url"]
      local n = string.match(newurl, "[%?&]n=([^&]+)")
      if n then
        local new_n = decrypted_ns[n]
        if not new_n then
          new_n = decrypt_n(n, body)
          decrypted_ns[n] = new_n
        end
        newurl = string.gsub(newurl, "([%?&]n=)[^&]+", "%1" .. string.gsub(new_n, "%-", "%%%-"))
      end
      allowed_urls[newurl] = true
      check(newurl)
      if not string.match(newurl, "&video_id=") then
        newurl = newurl .. "&video_id=" .. item_value
      end
      local stream_type_2 = string.match(stream_type, "^([a-z]+)")
      newurl = newurl .. "&stream_type=" .. stream_type_2
      if stream_type_2 == "audio" and audio_stream_count > 1 then
        local name = string.match(stream_type, "^[a-z]+ (.+)$")
        assert(current_audio_default ~= nil)
        newurl = newurl .. "&stream_name=" .. urlparse.escape(name) .. "&stream_is_default=" .. tostring(current_audio_default==name)
      end
      allowed_urls[newurl] = true
      check(newurl)
    end
  end

  local match = string.match(url, "^(https?://[^/]*ytimg%.com/.+maxresdefault.+)%?v=")
  if match then
    check(match)
  end

  if allowed(url, nil) and status_code == 200
    and not string.match(url, "^https?://[^/]*googlevideo%.com")
    and not string.match(url, "^https?://[^/]*ytimg%.com") then
    html = read_file(file)
    if url == "https://www.youtube.com/youtubei/v1/player" then
      queue_streams(cjson.decode(html))
    end
    if string.match(url, "^https?://[^/]*youtube%.com/watch%?v=[^&]+$") then
      check("https://youtu.be/" .. item_value)
      context["initial_data"] = cjson.decode(string.match(html, "<script[^>]+>var%s+ytInitialData%s*=%s*({.-})%s*;%s*</script>"))
      context["initial_player"] = cjson.decode(string.match(html, "<script[^>]+>var%s+ytInitialPlayerResponse%s*=%s*({.-})%s*;"))
      context["ytplayer"] = cjson.decode(string.match(html, "ytcfg%.set%(({.-})%)%s*;%s*window%.ytcfg%.obfuscatedData_"))
      if context["ytplayer"]["XSRF_FIELD_NAME"] ~= "session_token" then
        error("Could not find a session_token.")
      end
      -- INITIAL COMMENT CONTINUATION
      current_referer = url
      context["xsrf_token"] = context["ytplayer"]["XSRF_TOKEN"]
      post_headers = {
        ["Content-Type"]=nil,
        ["X-Youtube-Client-Name"]=context["ytplayer"]["INNERTUBE_CONTEXT_CLIENT_NAME"],
        ["X-Youtube-Client-Version"]=context["ytplayer"]["INNERTUBE_CONTEXT_CLIENT_VERSION"],
        Referer=current_referer
      }
      context["api_key"] = context["ytplayer"]["INNERTUBE_API_KEY"]
      context["api_version"] = context["ytplayer"]["INNERTUBE_API_VERSION"]
      set_current_context(context["ytplayer"]["INNERTUBE_CONTEXT"])
      local initial_data = context["initial_data"]["contents"]["twoColumnWatchNextResults"]["results"]["results"]["contents"]
      local found = false
      local found_info = false
      context["180"] = false
      context["360"] = false
      for _, d in pairs(initial_data) do
        data = d["videoPrimaryInfoRenderer"]
        if data and not found_info then
          found_info = true
          for _, run in pairs(data["title"]["runs"]) do
            run = run["text"]
            if run then
              if string.match(run, "180") then
                context["180"] = true
              elseif string.match(run, "360") then
                context["360"] = true
              end
            end
          end
          data = data["badges"]
          if data then
            for _, badge in pairs(data) do
              badge = badge["metadataBadgeRenderer"]
              if badge then
                if string.match(badge["label"], "180") then
                  context["180"] = true
                elseif string.match(badge["label"], "360") then
                  context["360"] = true
                end
              end
            end
          end
        end
        data = d["itemSectionRenderer"]
        if data and found_info then
          local continuations = data["continuations"]
          if continuations then
            queue_continuation_old(continuations, false, false)
            found = true
          end
          data = data["contents"]
          check_list_length(data)
          data = data[1]
          local message_renderer = data["messageRenderer"]
          local continuation_item_renderer = data["continuationItemRenderer"]
          if message_renderer
            and string.match(message_renderer["text"]["runs"][1]["text"], "Comments are turned off%.") then
            print("comments turned off")
            found = true
          elseif continuation_item_renderer then
            print("getting comments")
            queue_continuation_new(continuation_item_renderer, "both")
            found = true
          end
        end
      end
      if not found then
        error("Unsupported comments endpoint.")
      end
      -- IMAGE
      local current_height = nil
      local current_url = nil
      local current_max_height = nil
      local current_max_url = nil
      for _, data in pairs(context["initial_player"]["videoDetails"]["thumbnail"]["thumbnails"]) do
        local height = data["height"]
        if not current_url or height < current_height then
          current_height = height
          current_url = data["url"]
        end
        if (not current_max_url or height > current_max_height)
          and (item_type == "v1" or item_type == "v2") then
          current_max_height = height
          current_max_url = data["url"]
        end
      end
      if not current_url then
        error("Could not find a thumbnail.")
      else
        check(current_url)
        if current_max_url then
          check(current_max_url)
        end
      end
      -- VIDEO INITIAL
      if item_type == "v1" or item_type == "v2" then
        local get_type = "ios"
        if get_type == "mweb" then
        local mweb_agent = "Mozilla/5.0 (iPad; CPU OS 16_7_10 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Mobile/15E148 Safari/604.1,gzip(gfe)"
        local mweb_version = "2.20241202.07.00"
        allowed_urls["https://www.youtube.com/youtubei/v1/player"] = true
        table.insert(
          urls,
          {
            url="https://www.youtube.com/youtubei/v1/player",
            method="POST",
            body_data=cjson.encode({
              ["context"]={
                  ["client"]={
                    ["clientName"]="MWEB",
                    ["clientVersion"]=mweb_version,
                    ["userAgent"]=mweb_agent,
                    ["hl"]="en",
                    ["timeZone"]="UTC",
                    ["utcOffsetMinutes"]=0
                  }
              },
              ["videoId"]=item_value,
              ["playbackContext"]={
                  ["contentPlaybackContext"]={
                      ["html5Preference"]="HTML5_PREF_WANTS",
                      ["signatureTimestamp"]=context["ytplayer"]["STS"]
                  }
              },
              ["contentCheckOk"]=true,
              ["racyCheckOk"]=true
            }),
            headers={
              ["X-YouTube-Client-Name"]="2",
              ["X-YouTube-Client-Version"]=mweb_version,
              ["Origin"]="https://www.youtube.com",
              ["User-Agent"]=mweb_agent,
              ["content-type"]="application/json",
              ["X-Goog-Visitor-Id"]=context["ytplayer"]["VISITOR_DATA"]
            }
          }
        )
        elseif get_type == "ios" then
        local ios_agent = "com.google.ios.youtube/19.45.4 (iPhone16,2; U; CPU iOS 18_1_0 like Mac OS X;)"
        local ios_version = "19.45.4"
        allowed_urls["https://www.youtube.com/youtubei/v1/player"] = true
        table.insert(
          urls,
          {
            url="https://www.youtube.com/youtubei/v1/player",
            method="POST",
            body_data=cjson.encode({
              ["context"]={
                  ["client"]={
                    ["clientName"]="IOS",
                    ["clientVersion"]=ios_version,
                    ["deviceMake"]="Apple",
                    ["deviceModel"]="iPhone16,2",
                    ["userAgent"]=ios_agent,
                    ["osName"]="iPhone",
                    ["osVersion"]="18.1.0.22B83",
                    ["hl"]="en",
                    ["timeZone"]="UTC",
                    ["utcOffsetMinutes"]=0
                  }
              },
              ["videoId"]=item_value,
              ["playbackContext"]={
                  ["contentPlaybackContext"]={
                      ["html5Preference"]="HTML5_PREF_WANTS",
                      ["signatureTimestamp"]=context["ytplayer"]["STS"]
                  }
              },
              ["contentCheckOk"]=true,
              ["racyCheckOk"]=true
            }),
            headers={
              ["X-YouTube-Client-Name"]="5",
              ["X-YouTube-Client-Version"]=ios_version,
              ["Origin"]="https://www.youtube.com",
              ["User-Agent"]=iod_agent,
              ["content-type"]="application/json",
              ["X-Goog-Visitor-Id"]=context["ytplayer"]["VISITOR_DATA"]
            }
          }
        )
        else
          error("Should not reach this.")
        end
        -- old, directly from data in HTML
        -- queue_streams(context["initial_player"])
      end
      -- ADVERTISEMENT
      if context["initial_player"]["adSlots"] then
        for _, data in pairs(context["initial_player"]["adSlots"]) do
          for video_id in string.gmatch(cjson.encode(data), '"externalVideoId"%s*:%s*"([^"]+)"') do
            print("Found advertisement", video_id)
            discovered_self["v2:" .. video_id] = true
          end
        end
      end
      -- CAPTIONS
      local captions = context["initial_player"]["captions"]
      if captions then
        local translations = {""}
        local translation_versions = captions["playerCaptionsTracklistRenderer"]["translationLanguages"]
        if translation_versions then
          for _, data in pairs(translation_versions) do
            table.insert(translations, "&tlang=" .. data["languageCode"])
          end
        end
        for _, caption in pairs(captions["playerCaptionsTracklistRenderer"]["captionTracks"]) do
          local base_url = caption["baseUrl"]
          if base_url then
            check(base_url)
            for _, fmt in pairs({"json3", "vtt", "srt", "srv1", "srv2", "srv3", "ttml"}) do
              local local_translations = {""}
              --if caption["isTranslatable"] then
              --  local_translations = translations
              --end
              for _, translate_param in pairs(local_translations) do
                local newurl = base_url .. translate_param .. "&fmt=" .. fmt
                check(newurl)
                if fmt == "json3" then
                  check(newurl .. "&xorb=2&xobt=3&xovt=3")
                end
              end
            end
          end
        end
      end
      -- MORE VIDEOS
      -- TODO
    end
    -- OLD COMMENT STYLE
    if string.match(url, "^https?://[^/]*youtube%.com/comment_service_ajax") then
      local data = cjson.decode(html)
      local key_name = "itemSectionContinuation"
      local replies = false
      local with_next = false
      if string.match(url, "type=next") then
        with_next = true
      end
      -- NEXT COMMENT PAGE
      if not data["response"] then
        key_name = "commentRepliesContinuation"
        replies = true
        for _, d in pairs(data) do
          if d["response"] then
            data = d
            break
          end
        end
      end
      data = data["response"]["continuationContents"][key_name]
      if not sorted_new['dummy'] then
        -- GET NEWEST COMMENTS
        local new_data = data["header"]["commentsHeaderRenderer"]["sortMenu"]["sortFilterSubMenuRenderer"]["subMenuItems"]
        for _, d in pairs(new_data) do
          if d["title"] == "Newest first" then
            queue_continuation_old(d["continuation"], false, true)
            sorted_new['dummy'] = true
          end
        end
      else
        local continuation_data = data["continuations"]
        if continuation_data then
          queue_continuation_old(continuation_data, replies, false)
        end
        -- COMMENTS ON COMMENTS
        data = data["contents"]
        if key_name == "itemSectionContinuation" and data then
          for _, d in pairs(data) do
            d = d["commentThreadRenderer"]["replies"]
            if d then
              queue_continuation_old(d["commentRepliesRenderer"]["continuations"], true, false)
            end
          end
        end
      end
    end
    -- NEW COMMENT STYLE
    if string.match(url, "^https?://[^/]*youtube.com/youtubei/v1/next") then
      local data = cjson.decode(html)["onResponseReceivedEndpoints"]
      local just_sorted = false
      local pretty_print = "no"
      if string.match(url, "&prettyPrint=false") then
        pretty_print = "yes"
      elseif string.match(url, "prettyPrint=") then
        error("Should not have prettyPrint= in URL.")
      end
      if sorted_new[pretty_print] == nil then
        sorted_new[pretty_print] = false
      end
      for _, d in pairs(data) do
        local continuation_items_action = d["appendContinuationItemsAction"]
        if not continuation_items_action then
          continuation_items_action = d["reloadContinuationItemsCommand"]
        end
        if not continuation_items_action then
          error("Bad continuation_items_action found.")
        end
        local continuation_items = continuation_items_action["continuationItems"]
        if continuation_items then
          -- GET NEWEST COMMENTS
          for _, item in pairs(continuation_items) do
            if not sorted_new[pretty_print] then
              local comment_header_renderer = item["commentsHeaderRenderer"]
              if comment_header_renderer then
                for _, d in pairs(comment_header_renderer["sortMenu"]["sortFilterSubMenuRenderer"]["subMenuItems"]) do
                  --[[if not d["selected"] and d["title"] ~= "Newest first" then
                    error("Unknown ordering '" .. d["title"] .. "'.")
                  end]]
                  if d["title"] == "Newest first" then
                    if d["selected"] then
                      sorted_new[pretty_print] = true
                    else
                      queue_continuation_new(d["serviceEndpoint"], pretty_print)
                      sorted_new[pretty_print] = true
                      just_sorted = true
                    end
                  end
                end
              end
              if not sorted_new[pretty_print] then
                error("Could not sort on newest first.")
              end
            end
          end
          for _, item in pairs(continuation_items) do
            -- COMMENTS ON COMMENTS
            local comment_thread_renderer = item["commentThreadRenderer"]
            if comment_thread_renderer then
              local replies = comment_thread_renderer["replies"]
              if replies then
                print("getting replies")
                replies = replies["commentRepliesRenderer"]["contents"]
                check_list_length(replies)
                queue_continuation_new(replies[1]["continuationItemRenderer"], pretty_print)
              end
            end
            -- NEXT COMMENT PAGE
            if sorted_new[pretty_print] and not just_sorted then
              local continuation_item_renderer = item["continuationItemRenderer"]
              if continuation_item_renderer then
                print("getting more comments")
                queue_continuation_new(continuation_item_renderer, pretty_print)
              end
            else
            end
          end
        end
      end
    end
    html = urlparse.unescape(html)
    html = string.gsub(
      html, "\\[uU]([0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F])",
      function (s)
        return utf8.char(tonumber(s, 16))
      end
    )
    html = string.gsub(html, "\\/", "/")
    for name, type_ in pairs({
      ["[vV][iI][dD][eE][oO][iI][dD]"]="v",
      ["[cC][hH][aA][nN][nN][eE][lL][iI][dD]"]="ch",
      ["[pP][lL][aA][yY][lL][iI][sS][tT][iI][dD]"]="p"
    }) do
      for s in string.gmatch(html, name .. "[\"']%s*:%s*[\"'](" .. video_pattern .. ")[\"']") do
        queue_item(type_, s)
      end
      for s in string.gmatch(html, name .. "[sS][\"']%s*:%s*(%[[^%]]+%])") do
        for _, v in pairs(cjson.decode(s)) do
          queue_item(type_, v)
        end
      end
    end
    for s in string.gmatch(html, "[%?&]v=(" .. video_pattern .. ")") do
      queue_item("v", s)
    end
    for s in string.gmatch(html, "channel/(" .. channel_pattern .. ")") do
      queue_item("ch", s)
    end
    for s in string.gmatch(html, "user/(" .. user_pattern .. ")[\"']") do
      queue_item("u", s)
    end
    for s in string.gmatch(html, "[^0-9a-zA-Z%-_]c/(" .. user_pattern .. ")") do
      queue_item("c", s)
    end
    for s in string.gmatch(html, "[%?&]list=(" .. playlist_pattern .. ")") do
      queue_item("p", s)
    end
    if string.match(html, "/attribution%?v=" .. item_value) then
      check("https://www.youtube.com/attribution?v=" .. item_value)
    end
    -- TODO
    --[[for newurl in string.gmatch(string.gsub(html, "&quot;", '"'), '([^"]+)') do
      checknewurl(newurl)
    end
    for newurl in string.gmatch(string.gsub(html, "&#039;", "'"), "([^']+)") do
      checknewurl(newurl)
    end
    for newurl in string.gmatch(html, ">%s*([^<%s]+)") do
      checknewurl(newurl)
    end
    for newurl in string.gmatch(html, "[^%-]href='([^']+)'") do
      checknewshorturl(newurl)
    end
    for newurl in string.gmatch(html, '[^%-]href="([^"]+)"') do
      checknewshorturl(newurl)
    end
    for newurl in string.gmatch(html, "%(([^%)]+)%)") do
      checknewurl(newurl)
    end]]
  end

  return urls
end

wget.callbacks.httploop_result = function(url, err, http_stat)
  local function banned()
    print("you're likely banned, sleeping for 1800 seconds.")
    os.execute("sleep 1800")
  end

  status_code = http_stat["statcode"]

  set_new_item(url["url"])

  url_count = url_count + 1
  io.stdout:write(url_count .. "=" .. status_code .. " " .. url["url"] .. "  \n")
  io.stdout:flush()

  if killgrab then
    return wget.actions.ABORT
  end

  if status_code == 429 then
    if string.match(url["url"], "/api/timedtext") then
      os.execute("sleep 2")
    else
      banned()
      return wget.actions.ABORT
    end
  end

  if status_code >= 300 and status_code <= 399 then
    local newloc = urlparse.absolute(url["url"], http_stat["newloc"])
    if string.match(url["url"], "watch%?v=")
      or string.match(newloc, "consent%.youtube%.com")
      or string.match(newloc, "consent%.google%.com/")
      or string.match(newloc, "google%.com/sorry") then
      print("bad redirect to", newloc)
      banned()
      return wget.actions.ABORT
    end
    if downloaded[newloc] or addedtolist[newloc]
      or not allowed(newloc, url["url"]) then
      tries = 0
      return wget.actions.ABORT -- TODO
    end
  end

  if status_code >= 200 and status_code <= 399 then
    downloaded[url["url"]] = true
  end

  if abortgrab then
    abort_item()
    return wget.actions.ABORT
  end

  if status_code == 404
    and string.match(url["url"], "^https?://[^/]*ytimg%.com") then
    return wget.actions.NOTHING
  end

  if status_code == 0 or status_code >= 400 then
    io.stdout:write("Server returned " .. http_stat.statcode .. " (" .. err .. "). Sleeping.\n")
    io.stdout:flush()
    local maxtries = 2
    if tries >= maxtries then
      io.stdout:write("I give up...\n")
      io.stdout:flush()
      tries = 0
      abort_item()
      return wget.actions.ABORT
    else
      os.execute("sleep " .. math.floor(math.pow(2, tries)))
      tries = tries + 1
      return wget.actions.CONTINUE
    end
  end

  if string.match(url["url"], "^https?://[^/]*googlevideo%.com/videoplayback") then
    if seen_200[url["url"]] then
      io.stdout:write("Already attempted to download this URL.\n")
      io.stdout:flush()
      discovered_self["v1:" .. item_value] = true
      wget.callbacks.finish()
      return wget.actions.ABORT
    end
    seen_200[url["url"]] = true
  end

  tries = 0

  local sleep_time = 0

  if sleep_time > 0.001 then
    os.execute("sleep " .. sleep_time)
  end

  return wget.actions.NOTHING
end

wget.callbacks.finish = function(start_time, end_time, wall_time, numurls, total_downloaded_bytes, total_download_time)
  local function submit_backfeed(newurls, key)
    local tries = 0
    local maxtries = 4
    while tries < maxtries do
      if killgrab then
        return false
      end
      local body, code, headers, status = http.request(
        "https://legacy-api.arpa.li/backfeed/legacy/" .. key,
        newurls .. "\0"
      )
      print(body)
      if code == 200 then
        io.stdout:write("Submitted discovered URLs.\n")
        io.stdout:flush()
        break
      end
      io.stdout:write("Failed to submit discovered URLs." .. tostring(code) .. tostring(body) .. "\n")
      io.stdout:flush()
      os.execute("sleep " .. math.floor(math.pow(2, tries)))
      tries = tries + 1
    end
    if tries == maxtries then
      kill_grab()
    end
  end

  for key, data in pairs({
    ["youtube-stash-gdx8gc8jss2g68t"]=discovered, -- youtube-dww7l284444bgkw
    ["youtube-xpqppj8vq914e5yr"]=discovered_self,
    ["urls-iw1yksstlc7xgum"]=outlinks
  }) do
    local count = 0
    local items = nil
    for item, _ in pairs(data) do
      print("found item", item)
      if items == nil then
        items = item
      else
        items = items .. "\0" .. item
      end
      count = count + 1
      if count == 100 then
        submit_backfeed(items, key)
        items = nil
        count = 0
      end
    end
    if items ~= nil then
      submit_backfeed(items, key)
    end
  end
end

wget.callbacks.before_exit = function(exit_status, exit_status_string)
  if killgrab then
    return wget.exits.IO_FAIL
  end
  if abortgrab then
    abort_item()
    return wget.exits.IO_FAIL
  end
  return exit_status
end

