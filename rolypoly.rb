require 'aws-sdk'
require 'elasticsearch'
require 'httparty'
require 'socket'

ec2 = AWS::EC2.new

# useful variables
elasticsearch_cluster = ec2.instances.with_tag('es_cluster_name', 'es_test').filter('instance-state-name', 'running').map{ |i| i.private_ip_address }
start_time = Time.now.to_i

def get_relocating_shards(client)
  if client.cluster.health['relocating_shards'] != 0
    return false
  else
    return true
  end
end

def disable_allocation(client)
  puts "Disabling shard allocation on the cluster"
  client.cluster.put_settings body: { transient: { 'cluster.routing.allocation.disable_allocation' => true } }
end

def enable_allocation(client)
  puts "Enabling shard allocation on the cluster"
  client.cluster.put_settings body: { transient: { 'cluster.routing.allocation.disable_allocation' => false } }
end

def wait_for_relocating_shards(node, client)
  puts "Waiting for shards to settle on #{node}"
  until get_relocating_shards(client) do
    print "."
    sleep 1
  end
end

def restart_node(node)
  puts "Sending restart request to #{node}..."
  HTTParty.post("http://#{node}:9200/_cluster/nodes/_local/_shutdown")
  puts "Done."
end

def wait_for_http(node)
  puts "Waiting for elasticsearch to accept connections on #{node}:9200"
  until test_http(node) do
    print "."
    sleep 1
  end
end

def test_http(node)
  response = HTTParty.get("http://#{node}:9200", timeout: 1)
  if response['status'] == 200
    true
  end
  rescue Net::OpenTimeout, Errno::ECONNREFUSED
    sleep 1
    false
end

elasticsearch_cluster.each do |node|
  client = Elasticsearch::Client.new(host: node)
  if client.cluster.health['relocating_shards'] > 0
    puts "Are you nuts? Cluster is rebalancing! There are currently #{client.cluster.health['relocating_shards']} shards relocating. Quitting..."
    exit
  end
  disable_allocation(client)
  restart_node(node)
  # Wait for node to shutdown
  puts "Waiting 5s for node to initiate shutdown..."
  sleep 5
  wait_for_http(node)
  enable_allocation(client)
  wait_for_relocating_shards(node, client)
end

puts "Total restart time: #{Time.now.to_i - start_time}s"
