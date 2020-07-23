{application,emqx_bridge_kafka,
             [{description,"EMQ X KAFKA BRIDGE"},
              {vsn,"4.0.7"},
              {modules,[emqx_bridge_kafka,emqx_bridge_kafka_app,
                        emqx_bridge_kafka_sup]},
              {registered,[emqx_bridge_kafka_sup]},
              {applications,[kernel,stdlib,brod,supervisor3]},
              {mod,{emqx_bridge_kafka_app,[]}}]}.
