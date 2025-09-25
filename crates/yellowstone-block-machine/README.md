# Sans-IO Solana Block Reconsruction State Machine

This crates introduce a [sans-io](https://sans-io.readthedocs.io/) state machine to rebuild solana slot
properly, mainly from geyser events.

The state-machine encodes all the undocumented/implicit rules you must know about Geyser in order
to properly reconstruct a Slot. 

## Dragon's mouth integration

Visit [docs.rs](https://docs.rs/yellowstone-block-machine/latest/yellowstone_block_machine/) for more detailed example.

You can also look at the repo's [examples](https://github.com/rpcpool/yellowstone-block-machine/tree/main/examples/dragonsmouth) folder.


## Slot lifecycle Up to Processed

```

                                                                                                                                            
                                                                                                                                            
                                          TIME ->                                                                                           
      ──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────►   
      ┌───────────────────────────────────────────────────────┐                                                                             
      │ Slot download                                         │                                                                             
      │ ┌───────────┐┌──────┐         ┌───────┐┌───────────┐  │                                                                             
      │ │FIRST_SHRED││SHRED2│  ...    │SHRED N││ COMPLETED │  │                                                                             
      │ │ RECEIVED  │└──────┘         └───────┘└───────────┘  │                                                                             
      │ └───────────                                          │                                                                             
      └──────────────┌───────────────────────────────────────────────────────────────────────────────┐                                      
                     │ REPLAY STAGE                                                                  │                                      
                     │┌─────────────┐ ┌──────────────┐ ┌───┌───┐┌──────┐    ┌──────────┐ ┌─────────┐ │                                      
                     ││BANK_CREATED │ │ACCOUNT UPDATE│ │TX1│TX2││ENTRY1│... │BLOCK_META│ │PROCESSED│ │                                      
                     │└─────────────┘ └──────────────┘ └───└───┘└──────┘    └──────────┘ └─────────┘ │                                      
                     │                                                                               │                                      
                     └───────────────────────────────────────────────────────────────────────────────┘          
```

