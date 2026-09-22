-- Rewrite the inter-ADR markdown links. An ADR bound into this same PDF is
-- reached by an internal anchor; one that only has a source file of its own
-- points at its sibling PDF, so a folder of generated ADRs cross-references
-- itself; anything else (an ADR never written, or since removed) becomes plain
-- text rather than a dead link.
local siblings = {}
local internal = {}

local function read_meta(m)
  if m.siblings then
    for entry in pandoc.utils.stringify(m.siblings):gmatch("[^,]+") do
      siblings[entry:match("^(DX%-ADR%-%d+)")] = entry
    end
  end
  if m.internal then
    for key in pandoc.utils.stringify(m.internal):gmatch("[^,]+") do
      internal[key] = true
    end
  end
end

local function rewrite(el)
  local file, anchor = el.target:match("^([^#]*)(#?.*)$")
  if not file:match("%.md$") then
    return nil
  end
  local key = file:match("^(DX%-ADR%-%d+)") or ""
  if internal[key] then
    -- A heading anchor cannot survive the merge, so aim at the chapter itself.
    el.target = "#" .. key:lower()
    return el
  end
  local sibling = siblings[key]
  if not sibling then
    return el.content
  end
  el.target = sibling:gsub("%.md$", ".pdf") .. anchor
  return el
end

-- Meta is traversed after the document body, so the lists above have to be read
-- in a pass of their own before the links are rewritten.
return { { Meta = read_meta }, { Link = rewrite } }
