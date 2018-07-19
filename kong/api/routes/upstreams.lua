local endpoints   = require "kong.api.endpoints"
local responses = require "kong.tools.responses"
local utils = require "kong.tools.utils"
local public = require "kong.tools.public"


local function select_upstream(db, upstream_id)
  local id = ngx.unescape_uri(upstream_id)
  if utils.is_valid_uuid(id) then
    return db.upstreams:select({ id = id })
  end
  return db.upstreams:select_by_name(upstream_id)
end


local function select_target(db, target_id)
  local id = ngx.unescape_uri(target_id)
  if utils.is_valid_uuid(id) then
    return db.targets:select({ id = id })
  end
  return db.targets:select_by_target(target_id)
end


local function post_health(self, db, is_healthy)
  local upstream, _, err_t = select_upstream(db, self.params.upstreams)
  if err_t then
    return endpoints.handle_error(err_t)
  end
  local target, _, err_t = select_target(db, self.params.targets)
  if err_t then
    return endpoints.handle_error(err_t)
  end

  local ok, err = db.targets:post_health(upstream, target, is_healthy)
  if not ok then
    responses.send_HTTP_BAD_REQUEST(err)
  end

  return responses.send_HTTP_NO_CONTENT()
end


return {
  ["/upstreams/:upstreams/health/"] = {
    GET = function(self, db)
      local upstream, _, err_t = select_upstream(db, self.params.upstreams)
      if err_t then
        return endpoints.handle_error(err_t)
      end

      local targets_with_health = db.targets:for_upstream({ id = upstream.id }, { include_health = true })
      local node_id, err = public.get_node_id()
      if err then
        ngx.log(ngx.ERR, "failed getting node id: ", err)
      end

      return responses.send_HTTP_OK({
        data    = targets_with_health,
        node_id = node_id,
      })
    end
  },

  ["/upstreams/:upstreams/targets/all"] = {
    GET = function(self, db)
      local upstream, _, err_t = select_upstream(db, self.params.upstreams)
      if err_t then
        return endpoints.handle_error(err_t)
      end
      local targets = db.targets:for_upstream({ id = upstream.id }, { include_inactive = true })
      return responses.send_HTTP_OK({
        data  = targets,
      })
    end
  },

  ["/upstreams/:upstreams/targets/:targets/healthy"] = {
    POST = function(self, db)
      return post_health(self, db, true)
    end,
  },

  ["/upstreams/:upstreams/targets/:targets/unhealthy"] = {
    POST = function(self, db)
      return post_health(self, db, false)
    end,
  },
}
