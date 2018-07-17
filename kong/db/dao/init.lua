local cjson     = require "cjson"


local setmetatable = setmetatable
local tostring     = tostring
local require      = require
local error        = error
local pairs        = pairs
local floor        = math.floor
local type         = type
local log          = ngx.log
local fmt          = string.format


local ERR          = ngx.ERR


local new_tab
do
  local ok
  ok, new_tab = pcall(require, "table.new")
  if not ok then
    new_tab = function() return {} end
  end
end


local _M    = {}
local DAO   = {}
DAO.__index = DAO


local function validate_page_size(size)
  if type(size) ~= "number" or
    floor(size) ~= size or
    size < 1 or
    size > 1000 then
    error("size must be an integer between 0 and 1000", 3)
  end
end


local function validate_offset(offset)
  if type(offset) ~= "string" then
    error("offset must be a string", 3)
  end
end


local function validate_entity(entity)
  if type(entity) ~= "table" then
    error("entity must be a table", 3)
  end
end


local function validate_primary_key(primary_key)
  if type(primary_key) ~= "table" then
    error("primary_key must be a table", 3)
  end
end


local function validate_foreign_key(foreign_key)
  if type(foreign_key) ~= "table" then
    error("foreign_key must be a table", 3)
  end
end


local function validate_options(self, options, context)
  if options == nil then
    return
  end

  if options ~= nil and type(options) ~= "table" then
    error("options must be a table when specified", 3)
  end

  if self.schema.ttl == true and options.ttl ~= nil then
    if context ~= "insert" and context ~= "update" and context ~= "upsert" then
      error(fmt("ttl option can only be used with inserts, updates and upserts, not with '%ss'",
                tostring(context)), 3)
    end

    if type(options.ttl) ~= "number" or
      floor(options.ttl) ~= options.ttl or
            options.ttl  < 0 or
            options.ttl  > 100000000 then
      -- a bit over three years maximum to make it more safe against
      -- integer overflow (time() + ttl)
      error("ttl option must be an integer between 0 and 100000000", 3)
    end
  end
end


local function generate_foreign_key_methods(schema)
  local methods = {}

  for name, field in schema:each_field() do
    if field.type == "foreign" then
      local method_name = "for_" .. name

      methods[method_name] = function(self, foreign_key, size, offset, options)
        validate_foreign_key(foreign_key)

        if size ~= nil then
          validate_page_size(size)
        else
          size = 100
        end

        if offset ~= nil then
          validate_offset(offset)
        end

        validate_options(self, options, "select")

        local ok, errors = self.schema:validate_primary_key(foreign_key)
        if not ok then
          local err_t = self.errors:invalid_primary_key(errors)
          return nil, tostring(err_t), err_t
        end

        local strategy = self.strategy

        local rows, err_t, new_offset = strategy[method_name](strategy,
                                                              foreign_key,
                                                              size, offset)
        if not rows then
          return nil, tostring(err_t), err_t
        end

        local entities, err, err_t = self:rows_to_entities(rows)
        if err then
          return nil, err, err_t
        end

        return entities, nil, nil, new_offset
      end

    elseif field.unique then
      local function validate_unique_value(unique_value)
        local ok, err = schema:validate_field(field, unique_value)
        if not ok then
          error("invalid argument '" .. name .. "' (" .. err .. ")", 3)
        end
      end

      methods["select_by_" .. name] = function(self, unique_value, options)
        validate_unique_value(unique_value)
        validate_options(self, options, "select")

        local row, err_t = self.strategy:select_by_field(name, unique_value)
        if err_t then
          return nil, tostring(err_t), err_t
        end

        if not row then
          return nil
        end

        return self:row_to_entity(row)
      end

      methods["update_by_" .. name] = function(self, unique_value, entity, options)
        validate_unique_value(unique_value)
        validate_entity(entity)
        validate_options(self, options, "update")

        local entity_to_update, err = self.schema:process_auto_fields(entity, "update")
        if not entity_to_update then
          local err_t = self.errors:schema_violation(err)
          return nil, tostring(err_t), err_t
        end

        local ok, errors = self.schema:validate_update(entity_to_update)
        if not ok then
          local err_t = self.errors:schema_violation(errors)
          return nil, tostring(err_t), err_t
        end

        local row, err_t = self.strategy:update_by_field(name, unique_value,
                                                         entity_to_update, options)
        if not row then
          return nil, tostring(err_t), err_t
        end

        row, err, err_t = self:row_to_entity(row)
        if not row then
          return nil, err, err_t
        end

        self:post_crud_event("update", row)

        return row
      end

      methods["upsert_by_" .. name] = function(self, unique_value, entity, options)
        validate_unique_value(unique_value)
        validate_entity(entity)
        validate_options(self, options, "upsert")

        local entity_to_upsert, err = self.schema:process_auto_fields(entity, "upsert")
        if not entity_to_upsert then
          local err_t = self.errors:schema_violation(err)
          return nil, tostring(err_t), err_t
        end

        entity_to_upsert[name] = unique_value
        local ok, errors = self.schema:validate_upsert(entity_to_upsert)
        if not ok then
          local err_t = self.errors:schema_violation(errors)
          return nil, tostring(err_t), err_t
        end
        entity_to_upsert[name] = nil

        local row, err_t = self.strategy:upsert_by_field(name, unique_value,
                                                         entity_to_upsert, options)
        if not row then
          return nil, tostring(err_t), err_t
        end

        row, err, err_t = self:row_to_entity(row)
        if not row then
          return nil, err, err_t
        end

        self:post_crud_event("update", row)

        return row
      end

      methods["delete_by_" .. name] = function(self, unique_value, options)
        validate_unique_value(unique_value)
        validate_options(self, options, "delete")

        local entity, err, err_t = self["select_by_" .. name](self, unique_value, options)
        if err then
          return nil, err, err_t
        end
        if not entity then
          return true
        end

        local _, err_t = self.strategy:delete_by_field(name, unique_value, options)
        if err_t then
          return nil, tostring(err_t), err_t
        end

        self:post_crud_event("delete", entity)

        return true
      end
    end
  end

  return methods
end


function _M.new(db, schema, strategy, errors)
  local fk_methods = generate_foreign_key_methods(schema)
  local super      = setmetatable(fk_methods, DAO)

  local self = {
    db       = db,
    schema   = schema,
    strategy = strategy,
    errors   = errors,
    super    = super,
  }

  if schema.dao then
    local custom_dao = require(schema.dao)
    for name, method in pairs(custom_dao) do
      self[name] = method
    end
  end

  return setmetatable(self, { __index = super })
end


function DAO:select(primary_key, options)
  validate_primary_key(primary_key)
  validate_options(self, options, "select")

  local ok, errors = self.schema:validate_primary_key(primary_key)
  if not ok then
    local err_t = self.errors:invalid_primary_key(errors)
    return nil, tostring(err_t), err_t
  end

  local row, err_t = self.strategy:select(primary_key, options)
  if err_t then
    return nil, tostring(err_t), err_t
  end

  if not row then
    return nil
  end

  return self:row_to_entity(row)
end


function DAO:page(size, offset, options)
  if size ~= nil then
    validate_page_size(size)
  else
    size = 100
  end

  if offset ~= nil then
    validate_offset(offset)
  end

  validate_options(self, options, "select")

  local rows, err_t, offset = self.strategy:page(size, offset, options)
  if err_t then
    return nil, tostring(err_t), err_t
  end

  local entities, err, err_t = self:rows_to_entities(rows)
  if not entities then
    return nil, err, err_t
  end

  return entities, err, err_t, offset
end


function DAO:each(size, options)
  if size ~= nil then
    validate_page_size(size)
  else
    size = 100
  end

  validate_options(self, options, "select")

  local next_row = self.strategy:each(size, options)

  return function()
    local err_t
    local row, err, page = next_row()
    if not row then
      if err then
        if type(err) == "table" then
          return nil, tostring(err), err
        end

        local err_t = self.errors:database_error(err)
        return nil, tostring(err_t), err_t
      end

      return nil
    end

    row, err, err_t = self:row_to_entity(row)
    if not row then
      return nil, err, err_t
    end

    return row, nil, page
  end
end


function DAO:insert(entity, options)
  validate_entity(entity)
  validate_options(self, options, "insert")

  local entity_to_insert, err = self.schema:process_auto_fields(entity, "insert")
  if not entity_to_insert then
    local err_t = self.errors:schema_violation(err)
    return nil, tostring(err_t), err_t
  end

  local ok, errors = self.schema:validate_insert(entity_to_insert)
  if not ok then
    local err_t = self.errors:schema_violation(errors)
    return nil, tostring(err_t), err_t
  end

  local row, err_t = self.strategy:insert(entity_to_insert, options)
  if not row then
    return nil, tostring(err_t), err_t
  end

  row, err, err_t = self:row_to_entity(row)
  if not row then
    return nil, err, err_t
  end

  self:post_crud_event("create", row)

  return row
end


function DAO:update(primary_key, entity, options)
  validate_primary_key(primary_key)
  validate_entity(entity)
  validate_options(self, options, "update")

  local ok, errors = self.schema:validate_primary_key(primary_key)
  if not ok then
    local err_t = self.errors:invalid_primary_key(errors)
    return nil, tostring(err_t), err_t
  end

  local entity_to_update, err = self.schema:process_auto_fields(entity, "update")
  if not entity_to_update then
    local err_t = self.errors:schema_violation(err)
    return nil, tostring(err_t), err_t
  end

  ok, errors = self.schema:validate_update(entity_to_update)
  if not ok then
    local err_t = self.errors:schema_violation(errors)
    return nil, tostring(err_t), err_t
  end

  local row, err_t = self.strategy:update(primary_key, entity_to_update, options)
  if not row then
    return nil, tostring(err_t), err_t
  end

  row, err, err_t = self:row_to_entity(row)
  if not row then
    return nil, err, err_t
  end

  self:post_crud_event("update", row)

  return row
end


function DAO:upsert(primary_key, entity, options)
  validate_primary_key(primary_key)
  validate_entity(entity)
  validate_options(self, options, "upsert")

  local ok, errors = self.schema:validate_primary_key(primary_key)
  if not ok then
    local err_t = self.errors:invalid_primary_key(errors)
    return nil, tostring(err_t), err_t
  end

  local entity_to_upsert, err = self.schema:process_auto_fields(entity, "upsert")
  if not entity_to_upsert then
    local err_t = self.errors:schema_violation(err)
    return nil, tostring(err_t), err_t
  end

  ok, errors = self.schema:validate_upsert(entity_to_upsert)
  if not ok then
    local err_t = self.errors:schema_violation(errors)
    return nil, tostring(err_t), err_t
  end

  local row, err_t = self.strategy:upsert(primary_key, entity_to_upsert, options)
  if not row then
    return nil, tostring(err_t), err_t
  end

  row, err, err_t = self:row_to_entity(row)
  if not row then
    return nil, err, err_t
  end

  self:post_crud_event("update", row)

  return row
end


function DAO:delete(primary_key, options)
  validate_primary_key(primary_key)
  validate_options(self, options, "delete")

  local ok, errors = self.schema:validate_primary_key(primary_key)
  if not ok then
    local err_t = self.errors:invalid_primary_key(errors)
    return nil, tostring(err_t), err_t
  end

  local entity, err, err_t = self:select(primary_key, options)
  if err then
    return nil, err, err_t
  end
  if not entity then
    return true
  end

  local _, err_t = self.strategy:delete(primary_key, options)
  if err_t then
    return nil, tostring(err_t), err_t
  end

  self:post_crud_event("delete", entity)

  return true
end


function DAO:rows_to_entities(rows)
  local count = #rows
  if count == 0 then
    return setmetatable(rows, cjson.empty_array_mt)
  end

  local entities = new_tab(count, 0)

  for i = 1, count do
    local entity, err, err_t = self:row_to_entity(rows[i])
    if not entity then
      return nil, err, err_t
    end

    entities[i] = entity
  end

  return entities
end


function DAO:row_to_entity(row)
  local entity, errors = self.schema:process_auto_fields(row, "select")
  if not entity then
    local err_t = self.errors:schema_violation(errors)
    return nil, tostring(err_t), err_t
  end

  return entity
end


function DAO:post_crud_event(operation, entity)
  if self.events then
    local ok, err = self.events.post_local("dao:crud", operation, {
      operation = operation,
      schema    = self.schema,
      new_db    = true,
      entity    = entity,
    })
    if not ok then
      log(ERR, "[db] failed to propagate CRUD operation: ", err)
    end
  end

end


function DAO:cache_key(arg1, arg2, arg3, arg4, arg5)
  return fmt("%s:%s:%s:%s:%s:%s",
             self.schema.name,
             arg1 == nil and "" or arg1,
             arg2 == nil and "" or arg2,
             arg3 == nil and "" or arg3,
             arg4 == nil and "" or arg4,
             arg5 == nil and "" or arg5)
end


return _M
