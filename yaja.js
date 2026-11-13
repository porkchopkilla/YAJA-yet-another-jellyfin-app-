/* native-io.js: written by XITRIX for Moonlight-CivSwitch (https://github.com/XITRIX/Moonlight-CivSwitch),
 * copied unmodified at build time from Moonlight-CivSwitch-master/app/src/streaming/native-io.js.
 * Not covered by YAJA's license; all rights remain with its author. */
/* Translate the generated core's wasm32 socket ABI into CivSwitch SDK calls. */
(function(global){
 "use strict";
 function create(transport,options){
  options=options||{};
  var allocations=[],descriptors=new Set(),bufferSize=2097152,heapBuffer=null,heapView=null,heapAddress=0n,socketTypes={},interruptAfter=Infinity;
  var batchCore=null,batchHeap=0,batchFunction=null,batchSlots=[],batchSockets=new Map(),batchSize=32,packetSize=2048;
  var batchSlotBytes=batchSize*(packetSize+56+16);
  var directMetadata=0,directBase=0n,directCapacity=0;
  function now(){return global.performance&&global.performance.now?global.performance.now():Date.now();}
  function isInterrupted(){return interruptAfter!==Infinity&&now()>=interruptAfter;}
  function alloc(size){var p=malloc(size);if(!p)throw Error("Native streaming allocation failed");allocations.push(p);return p;}
  function invoke(name,args){return call_rop.apply(null,[transport.resolveNativeSymbol(name)].concat((args||[]).map(BigInt)));}
  function signed(value){return Number(BigInt.asIntN(32,value));}
  function call(name,args){var result=signed(invoke("_ZN2nn6socket"+name,args));return result<0?-Number(invoke("_ZN2nn6socket12GetLastErrnoEv",[])):result;}
  function copy(pointer,length){var bytes=new Uint8Array(length),view=new DataView(bytes.buffer),i=0;for(;i+8<=length;i+=8)view.setBigUint64(i,read64(pointer+BigInt(i)),true);for(;i<length;i++)bytes[i]=Number(read8(pointer+BigInt(i)));return bytes;}
  var address=alloc(128),addressLength=alloc(8),optionBuffer=alloc(128),optionLength=alloc(8);
  var packetCounts=new Uint32Array(65536),packetTypes=[];
  var stats={calls:0,sentBytes:0,receivedBytes:0,directReceivedBytes:0,directSentBytes:0,receiveCalls:0,receiveWouldBlock:0,nativeReceiveCalls:0,batchEnabled:false,batchReceiveCalls:0,batchPackets:0,batchCacheHits:0,batchCopiedBytes:0,lastError:0};
  // Format diagnostic keys only when sampled, not for every received packet.
  Object.defineProperty(stats,"rtpTypes",{enumerable:true,get:function(){var result={};for(var type of packetTypes)result[(type>>>8).toString(16)+":"+(type&255).toString(16)]=packetCounts[type];return result;}});
  function bounds(heap,pointer,length){pointer=pointer>>>0;if(!Number.isInteger(length)||length<0||pointer+length>heap.length)throw Error("Streaming heap range rejected");return pointer;}
  function completeHeap(heap){
   if(heap.byteOffset!==0||heap.byteLength!==heap.buffer.byteLength)throw Error("Expected complete core heap view");
  }
  function base(heap){
   completeHeap(heap);
   if(heap.buffer===heapBuffer)return heapAddress;
   var object=addrof_compressed(heap),external=read64_compressed(object+0x28n),compressedBase=read32_compressed(object+0x30n);
   if(compressedBase!==0n||external<0x100000000n||external>=0x8000000000n)throw Error("Core heap is not a validated external buffer");
   var bytes=copy(external,32);for(var i=0;i<32;i++)if(bytes[i]!==heap[i])throw Error("Core heap pointer verification failed");
   heapBuffer=heap.buffer;heapView=new DataView(heap.buffer);heapAddress=external;return external;
  }
  function toNative(heap,pointer,length,destination){pointer=bounds(heap,pointer,length);if(length)call_rop(memcpy_addr,destination,base(heap)+BigInt(pointer),BigInt(length));}
  function fromNative(heap,pointer,length,source){pointer=bounds(heap,pointer,length);if(length)call_rop(memcpy_addr,base(heap)+BigInt(pointer),source,BigInt(length));}
  function attachCore(core){
   if(batchCore)throw Error("Receive batching already attached");
   if(options.batchReceive===false||typeof sdk_base!=="bigint")return false;
   var fn;try{fn=transport.resolveNativeSymbol("_ZN2nn6socket8RecvMMsgEiPNS0_7MMsgHdrEmNS0_7MsgFlagEPNS_8TimeSpanE");}catch(_){return false;}
   // SDK 1.0.0 layout: 56-byte MMsgHdr; iovec pointer +16, count +24,
   // received length +48. Guard the observed implementation before using it.
   if(fn!==sdk_base+0x14d880n||read32(sdk_base+0x153720n)!==0xd10203ffn||read32(sdk_base+0x153724n)!==0xa9037bfdn)return false;
   batchCore=core;batchHeap=core._malloc(2*batchSlotBytes);if(!batchHeap)throw Error("Receive batch heap allocation failed");
   bounds(core.HEAPU8,batchHeap,2*batchSlotBytes);
   for(var i=0;i<2;i++){var offset=batchHeap+i*batchSlotBytes;batchSlots.push({fd:null,offset:offset,headerOffset:offset+batchSize*packetSize,vectorOffset:offset+batchSize*(packetSize+56),base:0n,count:0,next:0,lengths:new Uint32Array(batchSize)});}
   batchFunction=fn;stats.batchEnabled=true;return true;
  }
  function receiveBatch(fd,heap,destination,capacity,peek){
   var slot=batchSockets.get(fd);
   if(!slot){slot=batchSlots.find(function(s){return s.fd===null;});if(!slot)return null;slot.fd=fd;batchSockets.set(fd,slot);}
   bounds(heap,slot.offset,batchSlotBytes);
   if(slot.next>=slot.count){
    var address=base(heap),view=heapView;
    if(slot.base!==address){
     // Keep message metadata in the borrowed heap too. Reading result lengths
     // through DataView avoids an arbitrary native-memory read per datagram.
     heap.fill(0,slot.headerOffset,slot.offset+batchSlotBytes);
     for(var i=0;i<batchSize;i++){
      var header=slot.headerOffset+i*56,vector=slot.vectorOffset+i*16;
      view.setBigUint64(header+16,address+BigInt(vector),true);view.setUint32(header+24,1,true);
      view.setBigUint64(vector,address+BigInt(slot.offset+i*packetSize),true);view.setBigUint64(vector+8,BigInt(packetSize),true);
     }
     slot.base=address;
    }
    stats.nativeReceiveCalls++;stats.batchReceiveCalls++;
    var received=signed(call_rop(batchFunction,BigInt(fd),address+BigInt(slot.headerOffset),BigInt(batchSize),0x80n,0n));
    if(received<0)return -Number(invoke("_ZN2nn6socket12GetLastErrnoEv",[]));
    if(received>batchSize)throw Error("Native receive batch exceeded slot count");
    slot.count=received;slot.next=0;
    for(var i=0;i<received;i++){var length=view.getUint32(slot.headerOffset+i*56+48,true);if(length>packetSize)throw Error("Native receive batch exceeded packet capacity");slot.lengths[i]=length;}
    stats.batchPackets+=received;if(!received)return -11;
   }else stats.batchCacheHits++;
   var length=Math.min(capacity,slot.lengths[slot.next]),source=slot.offset+slot.next*packetSize;
   heap.copyWithin(destination,source,source+length);stats.batchCopiedBytes+=length;if(!peek)slot.next++;return length;
  }
  function receiveVideoBatch(fd,pointers,lengths,metadata,count,capacity,heap){
   if(isInterrupted())return -104;
   if(!batchFunction||options.directVideoBatch!==true)return -38;
   if(!descriptors.has(fd)||socketTypes[fd]!==2)return -9;
   if(count!==32||capacity<12||capacity>2048)return -22;
   completeHeap(heap);bounds(heap,pointers,count*4);bounds(heap,lengths,count*4);bounds(heap,metadata,count*72);
   var address=base(heap),view=heapView,vectors=metadata+count*56;
   // Validate all destinations before a native call can consume any datagrams.
   for(var i=0;i<count;i++)bounds(heap,view.getUint32(pointers+i*4,true),capacity);
   if(directMetadata!==metadata||directBase!==address||directCapacity!==capacity){
    heap.fill(0,metadata,metadata+count*72);
    for(var i=0;i<count;i++){
     view.setBigUint64(metadata+i*56+16,address+BigInt(vectors+i*16),true);view.setUint32(metadata+i*56+24,1,true);
     view.setUint32(vectors+i*16+8,capacity,true);
    }
    directMetadata=metadata;directBase=address;directCapacity=capacity;
   }
   // The validated Switch heap address is below 2**39, so adding a wasm32
   // offset is exact in Number. Avoid allocating BigInts for every packet.
   var numericBase=Number(address);
   for(var i=0;i<count;i++){
    var destination=numericBase+view.getUint32(pointers+i*4,true);
    view.setUint32(vectors+i*16,destination,true);view.setUint32(vectors+i*16+4,Math.floor(destination/4294967296),true);
   }
   stats.nativeReceiveCalls++;stats.directVideoBatchCalls=(stats.directVideoBatchCalls||0)+1;
   var received=signed(call_rop(batchFunction,BigInt(fd),address+BigInt(metadata),BigInt(count),0x80n,0n));
   if(received<0){var error=Number(invoke("_ZN2nn6socket12GetLastErrnoEv",[]));stats.lastError=error;if(error===11)stats.receiveWouldBlock++;return -error;}
   if(received>count)throw Error("Direct receive exceeded slot count");
   for(var i=0;i<received;i++){
    var length=view.getUint32(metadata+i*56+48,true);if(length>capacity)throw Error("Direct receive exceeded packet capacity");
    view.setUint32(lengths+i*4,length,true);stats.receivedBytes+=length;stats.directReceivedBytes+=length;
    if(length>=12){var p=view.getUint32(pointers+i*4,true),type=(heap[p]<<8)|heap[p+1];if(!packetCounts[type])packetTypes.push(type);packetCounts[type]++;}
   }
   stats.receiveCalls+=received;stats.directVideoPackets=(stats.directVideoPackets||0)+received;return received||-11;
  }
  function inputAddress(heap,pointer,length){
   bounds(heap,pointer,length);if(length<16||heap[pointer]!==2||heap[pointer+1]!==0)return false;
   var bytes=new Uint8Array(16);bytes.set(heap.subarray(pointer,pointer+16));bytes[0]=16;bytes[1]=2;write_buffer(address,bytes);return true;
  }
  function outputAddress(heap,pointer,lengthPointer){
   if(!pointer||!lengthPointer)return;
   bounds(heap,lengthPointer,4);var view=new DataView(heap.buffer),capacity=view.getUint32(lengthPointer,true),length=Math.min(capacity,16);
   bounds(heap,pointer,length);var bytes=copy(address,16);bytes[0]=2;bytes[1]=0;heap.set(bytes.subarray(0,length),pointer);view.setUint32(lengthPointer,16,true);
  }
  function nativeOption(level,option){
   if(level===1)return [0xffff,({2:4,4:0x1007,6:0x20,7:0x1001,8:0x1002,9:8,15:0x200})[option]||option];
   if(level===0)return [0,({1:3,2:4})[option]||option];
   return [level,option];
  }
  function io(op,args,heap){
   // Stop must not wait for network recovery or reliable-send/linger timers.
   // A fatal socket result (not EINTR, which ENet retries) unwinds C callers.
   if(isInterrupted()&&(op===3||op===4||op===5||op===11))return -104;
   stats.calls++;var a=args[0],b=args[1]>>>0,c=args[2]>>>0,d=args[3],e=args[4]>>>0,f=args[5]>>>0,result;
   try{
    if(op===1){
     if(a!==2)return -97;
     result=call("6SocketEiii",[a,b,c]);
     if(result>=0){var fd=result,flags=call("5FcntlEiiz",[fd,3,0]);result=flags<0?flags:call("5FcntlEiiz",[fd,4,flags|0x800]);if(result<0)call("5CloseEi",[fd]);else{descriptors.add(fd);socketTypes[fd]=b;result=fd;}}
    }else if(op===2||op===3){
     if(!inputAddress(heap,b,c))return -97;
     result=call(op===2?"4BindEiPK8sockaddrj":"7ConnectEiPK8sockaddrj",[a,address,16]);
    }else if(op===4||op===5){
     if(c>bufferSize)return -90;bounds(heap,b,c);
     // The socket call is synchronous, so this validated backing store stays
     // alive and cannot grow while native code reads/writes its borrowed range.
     completeHeap(heap);var flags=0x80|(d&2);
     if(op===4){
      var data=base(heap)+BigInt(b);
      if(e){if(!inputAddress(heap,e,f))return -97;result=call("6SendToEiPKvmiPK8sockaddrj",[a,data,c,flags,address,16]);}
      else result=call("4SendEiPKvmi",[a,data,c,flags]);
      if(result>0){if(result>c)throw Error("Native send exceeded borrowed range");stats.sentBytes+=result;stats.directSentBytes+=result;}
     }else{
      // Reject address ranges before a receive consumes a datagram.
      if(e&&f){bounds(heap,f,4);bounds(heap,e,Math.min(new DataView(heap.buffer).getUint32(f,true),16));}
      if(e)write32(addressLength,16n);stats.receiveCalls++;
      result=batchFunction&&socketTypes[a]===2&&!e&&c<=packetSize?receiveBatch(a,heap,b,c,!!(d&2)):null;
      if(result===null){var data=base(heap)+BigInt(b);stats.nativeReceiveCalls++;result=e?call("8RecvFromEiPvmiP8sockaddrPj",[a,data,c,flags,address,addressLength]):call("4RecvEiPvmi",[a,data,c,flags]);}
      if(result>=0){if(result>c)throw Error("Native receive exceeded borrowed range");outputAddress(heap,e,f);stats.receivedBytes+=result;stats.directReceivedBytes+=result;if(socketTypes[a]===2&&result>=12){var packetType=(heap[b]<<8)|heap[b+1];if(!packetCounts[packetType])packetTypes.push(packetType);packetCounts[packetType]++;}}
      else if(result===-11)stats.receiveWouldBlock++;
     }
    }else if(op===6){
     write32(addressLength,16n);result=call(d?"11GetPeerNameEiP8sockaddrPj":"11GetSockNameEiP8sockaddrPj",[a,address,addressLength]);
     if(result>=0)outputAddress(heap,b,c);
    }else if(op===7||op===8){
     var mapped=nativeOption(b,c),length=op===7?e:(bounds(heap,e,4),new DataView(heap.buffer).getUint32(e,true));
     if(length>128)return -22;bounds(heap,d>>>0,length);
     if(op===7){toNative(heap,d>>>0,length,optionBuffer);result=call("10SetSockOptEiiiPKvj",[a,mapped[0],mapped[1],optionBuffer,length]);}
     else{write32(optionLength,BigInt(length));result=call("10GetSockOptEiiiPvPj",[a,mapped[0],mapped[1],optionBuffer,optionLength]);if(result>=0){var actual=Number(read32(optionLength));if(actual>length)throw Error("Native socket option overflow");fromNative(heap,d>>>0,actual,optionBuffer);new DataView(heap.buffer).setUint32(e,actual,true);}}
    }else if(op===9)result=call("8ShutdownEii",[a,b]);
    else if(op===10){if(!descriptors.has(a))return -9;descriptors.delete(a);delete socketTypes[a];var slot=batchSockets.get(a);if(slot){slot.fd=null;slot.count=slot.next=0;batchSockets.delete(a);}result=call("5CloseEi",[a]);}
    else if(op===11){
     if(b>64)return -22;bounds(heap,a>>>0,b*8);
     // Both ABIs use an 8-byte {int fd, short events, short revents}. Like
     // recv(), zero-timeout poll borrows the validated heap synchronously.
     result=call("4PollEP6pollfdmi",[base(heap)+BigInt(a>>>0),b,0]);
     if(result>=0&&batchSockets.size){var view=new DataView(heap.buffer);for(var i=0;i<b;i++){var offset=(a>>>0)+i*8,slot=batchSockets.get(view.getInt32(offset,true));if(slot&&slot.next<slot.count&&(view.getInt16(offset+4,true)&1)){var events=view.getInt16(offset+6,true);if(!events)result++;view.setInt16(offset+6,events|1,true);}}}
    }else result=-38;
    if(result<0)stats.lastError=-result;return result;
   }catch(error){stats.lastError=5;throw error;}
  }
  return {io:io,receiveVideoBatch:receiveVideoBatch,stats:stats,invoke:invoke,copy:copy,toNative:toNative,fromNative:fromNative,heapAddress:base,attachCore:attachCore,
   interrupt:function(graceMs){interruptAfter=Math.min(interruptAfter,now()+Math.max(0,Math.min(100,Number(graceMs)||0)));},isInterrupted:isInterrupted,
   randomFill:function(target){var p=alloc(Math.max(1,target.length));try{invoke("_ZN2nn6crypto36GenerateCryptographicallyRandomBytesEPvm",[p,target.length]);target.set(copy(p,target.length));}finally{allocations.pop();free(p);}},
   close:function(){for(var fd of descriptors)call("5CloseEi",[fd]);descriptors.clear();batchSockets.clear();batchSlots=[];if(batchHeap){batchCore._free(batchHeap);batchHeap=0;}batchCore=null;batchFunction=null;for(var p of allocations.reverse())free(p);allocations=[];heapBuffer=null;heapView=null;}
  };
 }
 global.MoonlightStreamIO={create:create};
})(window);

;
/* native-texture.js: written by XITRIX for Moonlight-CivSwitch (https://github.com/XITRIX/Moonlight-CivSwitch),
 * copied unmodified at build time from Moonlight-CivSwitch-master/app/src/streaming/native-texture.js.
 * Not covered by YAJA's license; all rights remain with its author. */
/* Civ VII 1.0.0: owned RGBA texture presented through its live-image provider. */
(function(global){
 "use strict";
 var lookup=global.__moonlightTextureColorLookup||null,preparing=null;
 function validateNV12(heap,pointer,length,width,height,fields,rgba){
  function integer(n){return Number.isSafeInteger(n)&&n>=0;}
  if(!integer(pointer)||!integer(length)||pointer+length>heap.length||!integer(width)||!integer(height)||width<1||height<1||width>640||height>360||rgba.length<width*height)
   throw Error("Invalid native frame dimensions or bounds");
  var ys=fields.yStride,us=fields.uvStride,yo=fields.yOffset,uo=fields.uvOffset;
  if(![ys,us,yo,uo].every(integer)||ys<width||us<Math.ceil(width/2)*2||yo+(height-1)*ys+width>length||uo+(Math.ceil(height/2)-1)*us+Math.ceil(width/2)*2>length)
   throw Error("NV12 planes exceed decoded frame bounds");
 }
 function convertNV12(heap,pointer,length,width,height,fields,rgba){
  validateNV12(heap,pointer,length,width,height,fields,rgba);
  var ys=fields.yStride,us=fields.uvStride,yo=fields.yOffset,uo=fields.uvOffset;
  var out=0;
  for(var y=0;y<height;y++){
   var yp=pointer+yo+y*ys,uvp=pointer+uo+(y>>1)*us;
   for(var x=0;x<width;x++){
    var c=Math.max(0,heap[yp+x]-16),uv=uvp+(x&~1),d=heap[uv]-128,e=heap[uv+1]-128;
    var r=(298*c+409*e+128)>>8,g=(298*c-100*d-208*e+128)>>8,b=(298*c+516*d+128)>>8;
    r=r<0?0:r>255?255:r;g=g<0?0:g>255?255:g;b=b<0?0:b>255?255:b;
    rgba[out++]=(0xff000000|(b<<16)|(g<<8)|r)>>>0;
   }
  }
 }
 function prepare(){
  if(lookup)return Promise.resolve();if(preparing)return preparing;
  // Preserve all luma levels and six bits per chroma channel. This 4 MiB RAM
  // table avoids per-pixel multiplication/clamping in the jitless runtime.
  preparing=(async function(){
   var table=new Uint32Array(1<<20);
   for(var first=0;first<256;first+=4){
    for(var y=first;y<first+4;y++)for(var u=0;u<64;u++)for(var v=0;v<64;v++){
     var c=Math.max(0,y-16),d=(u===32?128:u*4+2)-128,e=(v===32?128:v*4+2)-128;
     var r=(298*c+409*e+128)>>8,g=(298*c-100*d-208*e+128)>>8,b=(298*c+516*d+128)>>8;
     r=r<0?0:r>255?255:r;g=g<0?0:g>255?255:g;b=b<0?0:b>255?255:b;
     table[(y<<12)|(u<<6)|v]=(0xff000000|(b<<16)|(g<<8)|r)>>>0;
    }
    await new Promise(function(resolve){setTimeout(resolve,0);});
   }
   lookup=table;global.__moonlightTextureColorLookup=table;
  })().catch(function(error){preparing=null;throw error;});return preparing;
 }
 function convertFast(heap,pointer,length,width,height,fields,rgba){
  if(!lookup)throw Error("Native texture colors are not prepared");
  validateNV12(heap,pointer,length,width,height,fields,rgba);
  var out=0;
  for(var y=0;y<height;y++){
   var yp=pointer+fields.yOffset+y*fields.yStride,uv=pointer+fields.uvOffset+(y>>1)*fields.uvStride;
   for(var x=0;x<width;x+=2){
    var chroma=((heap[uv+x]>>2)<<6)|(heap[uv+x+1]>>2);
    rgba[out++]=lookup[(heap[yp+x]<<12)|chroma];
    if(x+1<width)rgba[out++]=lookup[(heap[yp+x+1]<<12)|chroma];
   }
  }
 }
 function create(io,core,width,height,options){
  var gpu=!!(options&&options.gpu);
  if(!Number.isInteger(width)||!Number.isInteger(height)||width<1||height<1||width>(gpu?1920:640)||height>(gpu?1080:360))throw Error("Unsupported native texture dimensions");
  if(global.__moonlightNativeTextureOwner)throw Error("A native stream texture is already active");
  if(!gpu&&!lookup)throw Error("Prepare native texture colors before starting a stream");
  var allocations=[],handle=0n,provider=0n,published=false,closed=false,rgbaHeap=0,frames=0,uploadMs=0,maxUploadMs=0;
  var byteLength=width*height*4,info=0n,page=0n,pixels=0n,description=0n,name=0n;
  var sequence=(global.__moonlightNativeTextureSequence||0)+1;global.__moonlightNativeTextureSequence=sequence;
  var imageName="moonlight-experiment/"+sequence,url="live:/"+imageName,owner={};
  function alloc(n){var p=malloc(n);if(!p)throw Error("Native texture allocation failed");allocations.push(p);return p;}
  function invoke(offset,args){return call_rop.apply(null,[main_base+offset].concat(args));}
  function range(p,n,permissions){
   if(call_rop(sdk_base+0x168b0cn,info,page,p)&0x3fffffn)throw Error("Native texture memory query failed");
   var base=read64(info),end=base+read64(info+8n);
   if((Number(read32(info+24n))&permissions)!==permissions||p<base||p+BigInt(n)>end)throw Error("Native texture memory range rejected");
  }
  function pointer(p){range(p,8,1);return read64(p);}
  function close(){
   if(closed)return;closed=true;
   // Gameface owns its own reference after ReceiveUserImage. Its normal cache
   // release and Civ VII's deferred renderer deletion keep in-flight draws valid.
   if(published){invoke(0x4bc160n,[provider,name]);published=false;}
   if(handle){invoke(0x7817c0n,[handle]);handle=0n;}
   if(rgbaHeap){core._free(rgbaHeap);rgbaHeap=0;}
   for(var p of allocations.reverse())free(p);allocations=[];
   if(global.__moonlightNativeTextureOwner===owner)delete global.__moonlightNativeTextureOwner;
  }
  try{
   info=alloc(64);page=alloc(8);
   if(read32(sdk_base+0x168b10n)!==0xd40000c1n)throw Error("Unsupported memory query ABI");
   for(var signature of [[0x771750n,0xa9ba7bfdn],[0x7817c0n,0xd10103ffn],[0x4bbb90n,0xd101c3ffn],[0x4bc160n,0xd10183ffn],[0x771100n,0x72003c1fn],[0x7716d0n,0x90023249n],[0x4bc7a0n,0xa9bd7bfdn],[0x4bc570n,0xa9bc7bfdn]]){
    range(main_base+signature[0],8,5);
    if(read32(main_base+signature[0])!==signature[1])throw Error("Unsupported Civ VII native texture build");
   }
   provider=pointer(main_base+0x5e7bba0n+0x180n);range(provider,64,3);
   name=alloc(128);write_string(name,imageName);
   if(invoke(0x4bca00n,[provider,name])&0xffffn)throw Error("Native stream image name is already registered");
   pixels=alloc(byteLength);description=alloc(0x448);
   write32(description,28n);write32(description+4n,0n);write32(description+8n,1n);
   write32(description+0x10n,BigInt(width));write32(description+0x14n,BigInt(height));write32(description+0x18n,1n);write64(description+0x20n,pixels);
   if(!gpu){rgbaHeap=core._malloc(byteLength);if(!rgbaHeap)throw Error("RGBA presentation allocation failed");}
   global.__moonlightNativeTextureOwner=owner;
  }catch(error){close();throw error;}
  function ensureTexture(){
   if(!handle){
    handle=invoke(0x771750n,[description,name,1n,0x4an,0n])&0xffffn;
    if(handle<22n){handle=0n;throw Error("Native renderer rejected the stream texture");}
    var metadata=invoke(0x771100n,[handle]);range(metadata,64,1);
    if(read32(metadata+4n)!==BigInt(width)||read16(metadata+8n)!==BigInt(height)||read32(metadata+0x20n)!==28n)throw Error("Native stream texture metadata mismatch");
   }
  }
  function publish(){
   if(!published){invoke(0x4bbb90n,[provider,handle,name]);published=true;if(!invoke(0x4bc7a0n,[provider,handle]))throw Error("Cannot enable dynamic stream texture");}
   invoke(0x4bc570n,[provider,handle]);
  }
  function present(heap,pointer,length,fields){
   if(closed||gpu)throw Error("CPU stream texture is unavailable");
   var begin=Date.now(),existing=!!handle;
   convertFast(heap,pointer,length,width,height,fields,new Uint32Array(core.HEAPU8.buffer,rgbaHeap,width*height));
   io.toNative(core.HEAPU8,rgbaHeap,byteLength,pixels);
   ensureTexture();
   if(existing&&!invoke(0x7716d0n,[handle,description,name,0n,0x4an,0n]))throw Error("Native texture update failed");
   publish();
   var elapsed=Date.now()-begin;frames++;uploadMs+=elapsed;maxUploadMs=Math.max(maxUploadMs,elapsed);return url;
  }
  function target(){
   if(closed||!gpu)throw Error("GPU stream texture is unavailable");
   range(main_base+0x781a10n,8,5);if(read32(main_base+0x781a10n)!==0x92403c08n)throw Error("Unsupported native texture pointer getter");
   ensureTexture();var texture=invoke(0x781a10n,[handle]);range(texture,216,3);
   var device=pointer(pointer(main_base+0x4db9ed8n));range(device,8,1);
   return {texture:texture,device:device};
  }
  function presentNative(){
   if(closed||!gpu||!handle)throw Error("GPU stream texture is unavailable");
   // The caller must complete its synchronous transfer or observe its GPU fence
   // before publishing. The texture handle stays unchanged.
   publish();frames++;return url;
  }
  return {present:present,target:target,presentNative:presentNative,close:close,stats:function(){return {textureFrames:frames,textureUploadMs:uploadMs,maxTextureUploadMs:maxUploadMs};}};
 }
 global.MoonlightNativeTexture={create:create,prepare:prepare,convertNV12:convertNV12,convertFast:convertFast};
})(window);

;
/* native-media.js: written by XITRIX for Moonlight-CivSwitch (https://github.com/XITRIX/Moonlight-CivSwitch),
 * copied unmodified at build time from Moonlight-CivSwitch-master/app/src/streaming/native-media.js.
 * Not covered by YAJA's license; all rights remain with its author. */
/* Existing movie/Opus/AudioOut functions; all pointers refer to owned buffers. */
(function(global){
 "use strict";
 // Keep at most two decoder slots while draining; every acquisition is released
 // exactly once, including when format lookup or presentation fails.
 function drainLatest(options){
  var held=new Set(),latest=null,failure=null;
  function release(frame){options.release(frame);held.delete(frame.index);}
  try{
   for(var attempt=0;attempt<32;attempt++){
    var frame=options.acquire();if(!frame)break;
    if(held.has(frame.index))throw Error("Decoder returned an already acquired output slot");
    held.add(frame.index);options.decoded(frame);
    if(!frame.displayable){release(frame);continue;}
    if(!latest)latest=frame;
    else if(frame.timestamp>=latest.timestamp){release(latest);latest=frame;options.superseded();}
    else{release(frame);options.superseded();}
   }
   // An asynchronous presenter can explicitly take over the selected slot.
   if(latest)options.present(latest,function(){held.delete(latest.index);});
  }catch(error){failure=error;}
  finally{held.forEach(function(index){try{options.release({index:index});}catch(error){if(!failure)failure=error;}});}
  if(failure)throw failure;
 }
 function createGpuPipeline(options){
  var pending=null,failure=null;
  function submit(frame){
   if(pending||failure)throw Error("GPU transfer already pending or failed");pending=frame;
   try{options.submit(frame);}catch(error){failure=error;throw error;}
  }
  function finish(publish){
   if(failure)throw failure;if(!pending)return false;
   var ready;try{ready=options.ready();}catch(error){failure=error;throw error;}
   if(!ready)return false;
   var frame=pending,presentationFailure=null;pending=null;
   try{if(publish!==false)options.present(frame);}catch(error){presentationFailure=error;}
   try{options.release(frame);}catch(error){failure=error;throw error;}
   if(presentationFailure)throw presentationFailure;return true;
  }
  return {submit:submit,finish:finish,pending:function(){return !!pending;},close:function(){if(!finish(false)&&pending)throw Error("GPU transfer is still pending; native media retained");}};
 }
 function finishGpu(pipeline,clock,deadline){
  if(!pipeline)return false;
  if(pipeline.finish())return true;
  if(!Number.isFinite(deadline))return false;
  // Only nonblocking fence queries. Bound both elapsed time and query count;
  // a stalled clock or GPU never turns this into an unbounded render wait.
  for(var attempt=0;attempt<32&&pipeline.pending()&&clock()<deadline;attempt++)if(pipeline.finish())return true;
  return false;
 }
 function createVideoPacer(clock,period){
  var queue=[],primed=false,firstAt=null,last=null,lastTimestamp=-Infinity;
  function releaseAll(){var first=null;while(queue.length){try{last.release(queue.shift());}catch(error){if(!first)first=error;}}if(first)throw first;}
  function discard(reason){last.release(queue.shift());last.superseded(reason);}
  return {pending:function(){return queue.length;},close:releaseAll,drain:function(options){
   last=options;
   try{
    for(var attempt=0;attempt<32;attempt++){
     var frame=options.acquire();if(!frame)break;
     if(queue.some(function(f){return f.index===frame.index;}))throw Error("Decoder returned an already retained output slot");
     queue.push(frame);frame.queuedAt=clock();options.decoded(frame);
     if(!frame.displayable){queue.pop();options.release(frame);continue;}
     if(frame.timestamp<=lastTimestamp){queue.pop();options.release(frame);options.superseded();continue;}
     queue.sort(function(a,b){return a.timestamp-b.timestamp;});
     while(queue.length>2)discard("backlog");
    }
    while(queue.length&&clock()-(queue[0].submittedAt===undefined?queue[0].queuedAt:queue[0].submittedAt)>period*2)discard("age");
    if(!queue.length)return;
    if(firstAt===null)firstAt=clock();
    if(!primed&&queue.length<2&&clock()-firstAt<period)return;
    primed=true;var selected=queue.shift(),owned=true;lastTimestamp=selected.timestamp;
    try{options.present(selected,function(){owned=false;});}finally{if(owned)options.release(selected);}
   }catch(error){try{releaseAll();}catch(_){}throw error;}
  }};
 }
 function create(io,core,options){
  options=options||{};var owned=[],objects=[],decoder=0n,started=false,configured=false,outputStorage=0n,outputLength=0,outputHeap=0,width=0,height=0,fields=null;
  var movie=null,audio=null,texture=null,gpu=false,frameInterval=0,lastPreview=0,videoSubmitted=0,videoDecoded=0,presented=0,conversionMs=0,maxConversionMs=0;
  var pendingFrames=new Map(),decodeSamples=0,decodeMs=0,maxDecodeMs=0,lastPresentedAt=null,intervalCount=0,intervalMs=0,intervalSquaredMs=0,maxIntervalMs=0;
  var videoSuperseded=0,videoDeferredTicks=0,videoExpired=0,videoBacklogDrops=0,audioBatchSamples=960,audioPoolSize=2;
  var gpuTransfers=0,gpuTransferMs=0,nativePublishMs=0,gpuPolls=0,gpuPendingPolls=0,gpuPollMs=0,gpuTailChecks=0,gpuTailCompleted=0,gpuTailMs=0;
  var movieBase=0n,gpuFence=null,gpuPipeline=null,gpuSubmissionAt=0,gpuCompletionSamples=0,gpuCompletionMs=0,gpuSubmitCost=0,videoPacer=null,presentAgeMs=0,presentAgeSamples=0;
  function now(){return global.MoonlightTelemetry?global.MoonlightTelemetry.now():Date.now();}
  function presentedFrame(frame){var time=now();if(frame&&frame.submittedAt!==undefined){presentAgeMs+=time-frame.submittedAt;presentAgeSamples++;}if(lastPresentedAt!==null){var delta=time-lastPresentedAt;intervalCount++;intervalMs+=delta;intervalSquaredMs+=delta*delta;maxIntervalMs=Math.max(maxIntervalMs,delta);}lastPresentedAt=time;presented++;}
  function alloc(n){var p=malloc(n);if(!p)throw Error("Native media allocation failed");owned.push(p);return p;}
  function number(v){return Number(BigInt.asIntN(32,v));}
  function sdk(name,args){return io.invoke(name,args);}
  function findMovie(){
   if(movie)return;
   var info=alloc(64),page=alloc(8);
   if(read32(sdk_base+0x168b10n)!==0xd40000c1n)throw Error("Unsupported memory query ABI");
   function query(address){if(call_rop(sdk_base+0x168b0cn,info,page,address)&0x3fffffn)return null;return {base:read64(info),size:read64(info+8n),permissions:Number(read32(info+24n))};}
   function readable(address,length){var cursor=address,end=address+BigInt(length);for(var i=0;i<32&&cursor<end;i++){var q=query(cursor);if(!q||!(q.permissions&1)||q.base+q.size<=cursor)return false;cursor=q.base+q.size;}return cursor>=end;}
   var cursor=0n;
   for(var region=0;region<512&&cursor<0x8000000000n;region++){
    var q=query(cursor);if(!q||!q.size||q.base+q.size<=cursor)break;cursor=q.base+q.size;var base=q.base;
    if((q.permissions&5)!==5||base===main_base||base===sdk_base||!readable(base,8))continue;
    var offset=read32(base+4n);if(offset<1n||offset>0x10000000n)continue;
    var mod=base+offset;if(!readable(mod,8)||read32(mod)!==0x30444f4dn)continue;
    var dynamic=mod+BigInt.asIntN(32,read32(mod+4n)),tags={};
    for(var i=0;i<128;i++){var entry=dynamic+BigInt(i*16);if(!readable(entry,16))break;var tag=Number(read64(entry));if(!tag)break;tags[tag]=read64(entry+8n);}
    if(!tags[4]||!tags[5]||!tags[6])continue;for(var tag of [4,5,6])if(tags[tag]<base)tags[tag]+=base;
    var count=Number(read32(tags[4]+4n)),size=Number(tags[10]);
    if(count<1||count>10000||size<1||size>1048576||!readable(tags[6],count*24)||!readable(tags[5],size))continue;
    var symbols=io.copy(tags[6],count*24),strings=io.copy(tags[5],size),view=new DataView(symbols.buffer),exports={};
    for(var i=0;i<count;i++){var start=view.getUint32(i*24,true),name="";for(var j=start;j<strings.length&&j<start+4096&&strings[j];j++)name+=String.fromCharCode(strings[j]);var address=view.getBigUint64(i*24+8,true);if(address)exports[name]=base+address;}
    if(exports._ZN5movie7DecoderC1ENS_11DecoderTypeENS_19DecoderOutputFormatE){movie=exports;movieBase=base;break;}
   }
   if(!movie)throw Error("Native movie module unavailable");
  }
  function call(name,args){if(!movie[name])throw Error("Missing native movie function "+name);return call_rop.apply(null,[movie[name]].concat((args||[]).map(BigInt)));}
  function ok(name,args){var result=number(call(name,args));if(result)throw Error("Native video error 0x"+result.toString(16));}
  function object(type){var p=alloc(128);call("_ZN5movie"+type+"C1Ev",[p]);objects.push({type:type,p:p});return p;}
  function createGpuFence(device){
   var info=alloc(64),page=alloc(8);
   function range(p,n,permissions){if(call_rop(sdk_base+0x168b0cn,info,page,p)&0x3fffffn)throw Error("GPU sync memory query failed");if((Number(read32(info+24n))&permissions)!==permissions||p<read64(info)||p+BigInt(n)>read64(info)+read64(info+8n))throw Error("GPU sync pointer range rejected");}
   function resolve(offset){var cell=movieBase+BigInt(offset);range(cell,8,1);var pointer=read64(cell);range(pointer,8,1);var target=read64(pointer);range(target,16,5);return target;}
   var initialize=resolve(0x4038698),finalize=resolve(0x4038610),wait=resolve(0x4038720);
   if(read32(initialize)!==0xf900181fn||read32(initialize+4n)!==0xa9017c1fn||read32(finalize)!==0xb9400808n||read32(wait)!==0xd503237fn)throw Error("Unsupported GPU sync ABI");
   var pointer=alloc(256);if(!call_rop(initialize,pointer,device))throw Error("GPU sync initialization failed");
   return {pointer:pointer,ready:function(){var begin=now(),result=number(call_rop(wait,pointer,0n));gpuPolls++;gpuPollMs+=now()-begin;if(result===2){gpuPendingPolls++;return false;}if(result!==0&&result!==1)throw Error("GPU completion failed: "+result);return true;},close:function(){call_rop(finalize,pointer);}};
  }
  function integer(metadata,key){var k=alloc_string(key),v=alloc(8);try{if(!call("_ZNK5movie9MediaData9FindInt32EPKcPi",[metadata,k,v]))throw Error("Missing video format "+key);return number(read32(v));}finally{owned.pop();free(v);free(k);}}
  var config,input,output,format,index,available,pts,flags,size,alignment,spsNative,ppsNative;
  function videoSetup(videoFormat,w,h,fps){
   gpu=!!global.MoonlightNativeTexture&&options.renderer!=="png"&&options.renderer!=="native-texture";
   if(videoFormat!==1||w<64||h<64||w>(gpu?1920:640)||h>(gpu?1080:360))throw Error("Unsupported H.264 renderer dimensions");
   findMovie();width=w;height=h;frameInterval=gpu?0:Math.max(1,Math.floor(1000/fps));
   // The decoder installs allocator callbacks. Construct it before MediaData or
   // owned Buffer allocations so creation and destruction use the same allocator.
   decoder=alloc(1024);call("_ZN5movie7DecoderC1ENS_11DecoderTypeENS_19DecoderOutputFormatENS_11DecoderModeEi",[decoder,1,gpu?3:4,gpu?2:1,number(call("_ZN5movie17GetThreadPriorityEv",[]))]);
   objects.push({type:"7Decoder",p:decoder});if(!read64(decoder+8n))throw Error("Native decoder initialization failed");
   config=object("9MediaData");format=object("9MediaData");input=object("6Buffer");output=object("6Buffer");
   index=alloc(8);available=alloc(8);pts=alloc(8);flags=alloc(8);size=alloc(8);alignment=alloc(8);spsNative=alloc(65536);ppsNative=alloc(65536);
   if(global.MoonlightNativeTexture&&options.renderer!=="png")texture=global.MoonlightNativeTexture.create(io,core,w,h,{gpu:gpu});
   if(gpu){var target=texture.target();call("_ZN5movie6Buffer18SetDataAndCapacityEPvm",[output,target.texture,8]);call("_ZN5movie6Buffer9SetNvnDevEPv",[output,target.device]);}
   if(gpu&&!options.synchronousGpu){
    if(fps===60&&options.smoothVideo!==false)videoPacer=createVideoPacer(now,1000/fps);
    gpuFence=createGpuFence(target.device);call("_ZN5movie6Buffer10SetNvnSyncEPv",[output,gpuFence.pointer]);
    gpuPipeline=createGpuPipeline({
     submit:function(frame){var begin=now();ok("_ZN5movie7Decoder15GetOutputBufferEiPNS_6BufferE",[decoder,frame.index,output]);gpuSubmissionAt=now();gpuSubmitCost=gpuSubmissionAt-begin;gpuTransferMs+=gpuSubmitCost;conversionMs+=gpuSubmitCost;gpuTransfers++;},
     ready:gpuFence.ready,
     present:function(frame){var begin=now();gpuCompletionSamples++;gpuCompletionMs+=begin-gpuSubmissionAt;var url=texture.presentNative(),elapsed=now()-begin;nativePublishMs+=elapsed;conversionMs+=elapsed;maxConversionMs=Math.max(maxConversionMs,gpuSubmitCost+elapsed);if(options.onImage)options.onImage(url);lastPreview=begin;presentedFrame(frame);},
     release:function(frame){ok("_ZN5movie7Decoder24ReleaseOutputBufferIndexEi",[decoder,frame.index]);}
    });
   }
   if(options.onStatus)options.onStatus("Video decoder ready: "+w+"×"+h+" at "+fps+" fps");return 0;
  }
  function rawNal(heap,pointer,length){if(length>=4&&heap[pointer]===0&&heap[pointer+1]===0){if(heap[pointer+2]===1)return [pointer+3,length-3];if(heap[pointer+2]===0&&heap[pointer+3]===1)return [pointer+4,length-4];}return [pointer,length];}
  function videoFrame(pointer,length,kind,presentation,sps,spsLength,pps,ppsLength,heap){
   if(!decoder)return -1;
   if(!configured){
    if(!sps||!pps)return -1;var a=rawNal(heap,sps,spsLength),b=rawNal(heap,pps,ppsLength);if(a[1]<4||b[1]<1||a[1]>65536||b[1]>65536)return -1;
    io.toNative(heap,a[0],a[1],spsNative);io.toNative(heap,b[0],b[1],ppsNative);
    if(!call("_ZN5movie34CreateH264DecoderConfigurationDataEPKhmS1_mPNS_9MediaDataE",[spsNative,a[1],ppsNative,b[1],config]))throw Error("Native H.264 configuration failed");
    var key=alloc_string("max-input-size");try{call("_ZN5movie9MediaData8SetInt32EPKci",[config,key,width>1280||height>720?1048576:262144]);}finally{free(key);}
    ok("_ZN5movie7Decoder9ConfigureEPNS_9MediaDataE",[decoder,config]);ok("_ZN5movie7Decoder5StartEv",[decoder]);configured=started=true;
   }
   var result=number(call("_ZN5movie7Decoder23AcquireInputBufferIndexEPiS1_",[decoder,index,available]));if(result===0x1011)return -1;if(result)throw Error("Video input acquisition failed: "+result);
   var slot=number(read32(index));if(slot<0)return -1;
   ok("_ZN5movie7Decoder14GetInputBufferEiPNS_6BufferE",[decoder,slot,input]);
   var capacity=Number(call("_ZNK5movie6Buffer8CapacityEv",[input])),data=call("_ZNK5movie6Buffer4DataEv",[input]);if(!data||length<1||length>capacity)throw Error("Compressed video exceeds native input buffer");
   io.toNative(heap,pointer,length,data);var submittedAt=now();ok("_ZN5movie7Decoder24SendInputBufferForDecodeEiiilj",[decoder,slot,0,length,Math.round(presentation),0]);
   pendingFrames.set(String(Math.round(presentation)),submittedAt);if(pendingFrames.size>256)pendingFrames.delete(pendingFrames.keys().next().value);
   videoSubmitted++;return 0;
  }
  function drainVideo(){
   if(!started)return false;
   // A retained transfer owns this tick even if its fence completes here.
   if(gpuPipeline&&gpuPipeline.pending()){videoDeferredTicks++;gpuPipeline.finish();return true;}
   var priorTransfers=gpuTransfers,priorPresented=presented;
   var drain=videoPacer?videoPacer.drain:drainLatest;
   drain({
    acquire:function(){
     var result=number(call("_ZN5movie7Decoder24AcquireOutputBufferIndexEPiPlPjS1_",[decoder,index,pts,flags,available]));if(result===0x100e)return null;if(result)throw Error("Video output acquisition failed: "+result);
     var slot=number(read32(index));if(slot<0)return null;
     return {index:slot,timestamp:Number(BigInt.asIntN(64,read64(pts))),displayable:!(Number(read32(flags))&4)};
    },
    release:function(frame){ok("_ZN5movie7Decoder24ReleaseOutputBufferIndexEi",[decoder,frame.index]);},
    superseded:function(reason){videoSuperseded++;if(reason==="age")videoExpired++;if(reason==="backlog")videoBacklogDrops++;},
    decoded:function(frame){
     var stamp=String(frame.timestamp),submittedAt=pendingFrames.get(stamp);
     if(submittedAt!==undefined){frame.submittedAt=submittedAt;var delay=now()-submittedAt;decodeSamples++;decodeMs+=delay;maxDecodeMs=Math.max(maxDecodeMs,delay);pendingFrames.delete(stamp);}
     videoDecoded++;
    },
    present:function(frame,adopt){
     if(now()-lastPreview<(texture?frameInterval:200)||(options.canPresent&&!options.canPresent()))return;
     var slot=frame.index;
     if(!fields){
      ok("_ZN5movie7Decoder15GetOutputFormatEPNS_9MediaDataE",[decoder,format]);
      fields={width:integer(format,"width"),height:integer(format,"height")};
      if(fields.width!==width||fields.height!==height)throw Error("Unexpected stream output dimensions");
     }
     if(!gpu&&!outputStorage){
      Object.assign(fields,{yStride:integer(format,"nv12-y-stride"),uvStride:integer(format,"nv12-uv-stride"),yOffset:integer(format,"nv12-y-offset"),uvOffset:integer(format,"nv12-uv-offset")});
      ok("_ZN5movie7Decoder19GetOutputBufferSizeEPiS1_",[decoder,size,alignment]);outputLength=number(read32(size));if(outputLength<1||outputLength>4194304)throw Error("Invalid video output capacity");
      outputStorage=(alloc(outputLength+4096)+4095n)&~4095n;outputHeap=core._malloc(outputLength);if(!outputHeap)throw Error("Cannot allocate video presentation buffer");
      call("_ZN5movie6Buffer18SetDataAndCapacityEPvm",[output,outputStorage,outputLength]);
     }
     if(gpu){
       if(gpuPipeline){adopt();gpuPipeline.submit(frame);return;}
       var begin=now();ok("_ZN5movie7Decoder15GetOutputBufferEiPNS_6BufferE",[decoder,slot,output]);
       var copied=now(),url=texture.presentNative(),published=now(),elapsed=published-begin;gpuTransfers++;gpuTransferMs+=copied-begin;nativePublishMs+=published-copied;conversionMs+=elapsed;maxConversionMs=Math.max(maxConversionMs,elapsed);
       if(options.onImage)options.onImage(url);lastPreview=begin;presentedFrame(frame);
       return;
     }
     ok("_ZN5movie7Decoder15GetOutputBufferEiPNS_6BufferE",[decoder,slot,output]);
      var data=call("_ZNK5movie6Buffer4DataEv",[output]),used=Number(call("_ZNK5movie6Buffer4SizeEv",[output]));if(!data||used>outputLength||used<1)throw Error("Invalid decoded frame bounds");
      io.fromNative(core.HEAPU8,outputHeap,used,data);
      var begin=now(),url;
      if(texture)url=texture.present(core.HEAPU8,outputHeap,used,fields);
      else if(global.MoonlightPreview)url=global.MoonlightPreview.encode(core.HEAPU8,outputHeap,used,width,height,fields.yStride,fields.uvStride,fields.yOffset,fields.uvOffset);
      else {var encoded=core._ml_png_nv12(outputHeap,used,width,height,fields.yStride,fields.uvStride,fields.yOffset,fields.uvOffset);if(!encoded)throw Error("Frame presentation conversion failed");url="data:image/png;base64,"+core.UTF8ToString(encoded);}
      var elapsed=now()-begin;conversionMs+=elapsed;maxConversionMs=Math.max(maxConversionMs,elapsed);
      if(options.onImage)options.onImage(url);lastPreview=begin;presentedFrame(frame);
    }
   });
   return gpuTransfers!==priorTransfers||presented!==priorPresented;
  }
  function audioSetup(rate,channels,streams,coupled,samples){
   if(rate!==48000||channels!==2||streams!==1||coupled!==1)throw Error("First audio adapter supports stereo Opus at 48 kHz");
   var stateBytes=number(sdk("nncodecLibopus_opus_decoder_get_size",[2]));if(stateBytes<1||stateBytes>1048576)throw Error("Invalid Opus state size");
   audio={state:alloc(stateBytes),packet:alloc(65536),staging:alloc((5760+audioBatchSamples)*4),pending:alloc(audioBatchSamples*4),pendingSamples:0,stagedSamples:0,out:alloc(1024),parameter:alloc(64),buffers:[],opened:false,started:false,decoded:0,queued:0,dropped:0,catchups:0,discardedSamples:0,emptyEpisodes:0,emptySince:null,emptyMs:0};
   if(number(sdk("nncodecLibopus_opus_decoder_init",[audio.state,48000,2])))throw Error("Opus decoder initialization failed");
   sdk("_ZN2nn5audio27InitializeAudioOutParameterEPNS0_17AudioOutParameterE",[audio.parameter]);write32(audio.parameter,48000n);write16(audio.parameter+4n,2n);
   var result=sdk("_ZN2nn5audio19OpenDefaultAudioOutEPNS0_8AudioOutERKNS0_17AudioOutParameterE",[audio.out,audio.parameter]);if(result&0x3fffffn)throw Error("AudioOut failed: 0x"+(result&0x3fffffn).toString(16));audio.opened=true;
   for(var i=0;i<audioPoolSize;i++)audio.buffers.push({node:alloc(64),data:(alloc(0x7000)+4095n)&~4095n,inUse:false,samples:0});
   // Prime both 20 ms buffers before starting the device. This is a one-time
   // startup cushion, not a larger steady-state queue or a restart on starvation.
   return 0;
  }
  function reclaimAudio(){
   if(!audio.started)return;
   for(var i=0;i<audio.buffers.length;i++){
    var released=sdk("_ZN2nn5audio25GetReleasedAudioOutBufferEPNS0_8AudioOutE",[audio.out]);if(!released)break;
    var item=audio.buffers.find(function(item){return item.node===released;});if(!item||!item.inUse)throw Error("AudioOut returned an unowned buffer");item.inUse=false;
   }
   // Observed queue-empty periods are starvation indicators, not exact DSP
   // underrun durations: reclamation itself is sampled by the JS scheduler.
   if(!audio.buffers.some(function(item){return item.inUse;})&&audio.emptySince===null){audio.emptySince=now();audio.emptyEpisodes++;}
  }
  function queueAudio(buffer,source){
   var bytes=audioBatchSamples*4;
   call_rop(memcpy_addr,buffer.data,source,BigInt(bytes));
   sdk("_ZN2nn2os14FlushDataCacheEPKvm",[buffer.data,bytes]);write64(buffer.node,0n);write64(buffer.node+0x20n,0n);
   sdk("_ZN2nn5audio21SetAudioOutBufferInfoEPNS0_14AudioOutBufferEPvmm",[buffer.node,buffer.data,0x6000,bytes]);
   if(!sdk("_ZN2nn5audio20AppendAudioOutBufferEPNS0_8AudioOutEPNS0_14AudioOutBufferE",[audio.out,buffer.node]))throw Error("Native audio queue rejected PCM");
   buffer.inUse=true;buffer.samples=audioBatchSamples;audio.queued++;
   if(audio.emptySince!==null){audio.emptyMs+=Math.max(0,now()-audio.emptySince);audio.emptySince=null;}
   if(!audio.started&&audio.buffers.every(function(item){return item.inUse;})){
    var result=sdk("_ZN2nn5audio13StartAudioOutEPNS0_8AudioOutE",[audio.out]);
    if(result&0x3fffffn)throw Error("AudioOut start failed: 0x"+(result&0x3fffffn).toString(16));audio.started=true;
   }
  }
  function discardPendingAudio(){
   if(!audio.pendingSamples)return;
   audio.discardedSamples+=audio.pendingSamples;audio.dropped++;audio.catchups++;audio.pendingSamples=0;
  }
  function drainAudio(){
   if(!audio||!audio.opened)return;
   reclaimAudio();if(!audio.pendingSamples)return;var buffer=audio.buffers.find(function(item){return !item.inUse;});
   if(buffer){queueAudio(buffer,audio.pending);audio.pendingSamples=0;}
  }
  function audioFrame(pointer,length,heap){
   if(!audio||!audio.opened)return;
   // The retained partial chunk is below 960 samples; staging reserves a full
   // 5760-sample decode after it. Decode there directly instead of copying PCM
   // through an intermediate allocation on every Opus packet.
   io.toNative(heap,pointer,length,audio.packet);var samples=number(sdk("nncodecLibopus_opus_decode",[audio.state,audio.packet,length,audio.staging+BigInt(audio.stagedSamples*4),5760,0]));if(samples<0)throw Error("Native Opus decode failed: "+samples);audio.decoded++;
   if(samples>5760)throw Error("Native Opus output exceeds PCM capacity");
   audio.stagedSamples+=samples;
   // Two 20 ms native buffers plus one replaceable latest chunk. Keep Opus
   // state and active AudioOut buffers intact; drop only stale software PCM.
   var consumed=0;
   while(audio.stagedSamples-consumed>=audioBatchSamples){
    reclaimAudio();var buffer=audio.buffers.find(function(item){return !item.inUse;}),source=audio.staging+BigInt(consumed*4);consumed+=audioBatchSamples;
    // Preserve continuity when reclaimed capacity can hold the waiting chunk.
    // Replace it only when the native queue really has no room.
    if(buffer&&audio.pendingSamples){queueAudio(buffer,audio.pending);audio.pendingSamples=0;buffer=audio.buffers.find(function(item){return !item.inUse;});}
    if(buffer)queueAudio(buffer,source);
    else{discardPendingAudio();call_rop(memcpy_addr,audio.pending,source,BigInt(audioBatchSamples*4));audio.pendingSamples=audioBatchSamples;}
   }
   if(consumed){audio.stagedSamples-=consumed;if(audio.stagedSamples)call_rop(memcpy_addr,audio.staging,audio.staging+BigInt(consumed*4),BigInt(audio.stagedSamples*4));}
  }

  function close(){
   if(gpuPipeline)gpuPipeline.close();
   if(videoPacer){videoPacer.close();videoPacer=null;}
   if(audio&&audio.opened){if(audio.started)sdk("_ZN2nn5audio12StopAudioOutEPNS0_8AudioOutE",[audio.out]);sdk("_ZN2nn5audio20FlushAudioOutBuffersEPNS0_8AudioOutE",[audio.out]);sdk("_ZN2nn5audio13CloseAudioOutEPNS0_8AudioOutE",[audio.out]);}
   if(started)call("_ZN5movie7Decoder4StopEv",[decoder]);
   for(var entry of objects.reverse())call("_ZN5movie"+entry.type+"D1Ev",[entry.p]);objects=[];
   if(gpuFence){gpuFence.close();gpuFence=null;}gpuPipeline=null;
   if(texture){texture.close();texture=null;}
   if(outputHeap)core._free(outputHeap);outputHeap=0;core._ml_png_release();
   for(var p of owned.reverse())free(p);owned=[];started=false;configured=false;decoder=0n;audio=null;pendingFrames.clear();
  }
  return {videoSetup:videoSetup,videoFrame:videoFrame,audioSetup:audioSetup,audioFrame:audioFrame,drainVideo:drainVideo,finishVideo:function(deadline){var tail=Number.isFinite(deadline)&&gpuPipeline&&gpuPipeline.pending(),begin=tail?now():0,result=finishGpu(gpuPipeline,now,deadline);if(tail){gpuTailChecks++;if(result)gpuTailCompleted++;gpuTailMs+=now()-begin;}return result;},drainAudio:drainAudio,close:close,
   stats:function(){return Object.assign({renderer:texture?(gpu?"native-gpu-texture":"native-texture"):"png",submitted:videoSubmitted,decoded:videoDecoded,presented:presented,videoSuperseded:videoSuperseded,videoDeferredTicks:videoDeferredTicks,videoExpired:videoExpired,videoBacklogDrops:videoBacklogDrops,gpuTransfers:gpuTransfers,gpuTransferMs:gpuTransferMs,nativePublishMs:nativePublishMs,gpuPolls:gpuPolls,gpuPendingPolls:gpuPendingPolls,gpuPollMs:gpuPollMs,gpuTailChecks:gpuTailChecks,gpuTailCompleted:gpuTailCompleted,gpuTailMs:gpuTailMs,gpuCompletionSamples:gpuCompletionSamples,gpuCompletionMs:gpuCompletionMs,videoPacerPending:videoPacer?videoPacer.pending():0,presentAgeMs:presentAgeMs,presentAgeSamples:presentAgeSamples,conversionMs:conversionMs,maxConversionMs:maxConversionMs,decodeSamples:decodeSamples,decodeMs:decodeMs,maxDecodeMs:maxDecodeMs,decoderPending:Math.max(0,videoSubmitted-videoDecoded),intervalCount:intervalCount,intervalMs:intervalMs,intervalSquaredMs:intervalSquaredMs,maxIntervalMs:maxIntervalMs,audioBufferedMsUpperBound:audio?(audio.stagedSamples+audio.pendingSamples+audio.buffers.reduce(function(sum,item){return sum+(item.inUse?item.samples:0);},0))/48:0,audioQueueLimitMs:audioBatchSamples*(audioPoolSize+1)/48,audioCatchups:audio?audio.catchups:0,audioDiscardedMs:audio?audio.discardedSamples/48:0,audioDecoded:audio?audio.decoded:0,audioQueued:audio?audio.queued:0,audioDropped:audio?audio.dropped:0,audioPrimed:!!(audio&&audio.started),audioPrefillMs:audioBatchSamples*audioPoolSize/48,audioEmptyEpisodes:audio?audio.emptyEpisodes:0,audioEmptyMs:audio?audio.emptyMs+(audio.emptySince===null?0:Math.max(0,now()-audio.emptySince)):0},texture?texture.stats():{});}};
 }
 global.MoonlightStreamMedia={create:create,drainLatest:drainLatest,createGpuPipeline:createGpuPipeline,finishGpu:finishGpu,createVideoPacer:createVideoPacer};
})(window);

;
/* HTTP/1.1 over an injected byte transport: request builder, incremental response
 * parser (content-length, chunked, close-delimited) and convenience helpers. */
(function () {
    "use strict";
    var G = typeof window !== "undefined" ? window : globalThis;
    var JF = G.JF = G.JF || {};

    function encodeText(text) {
        var out = new Uint8Array(text.length * 3), n = 0;
        for (var i = 0; i < text.length; i++) {
            var c = text.charCodeAt(i);
            if (c >= 0xd800 && c < 0xdc00 && i + 1 < text.length) { c = 0x10000 + ((c - 0xd800) << 10) + (text.charCodeAt(++i) - 0xdc00); }
            if (c < 0x80) out[n++] = c;
            else if (c < 0x800) { out[n++] = 0xc0 | c >> 6; out[n++] = 0x80 | c & 63; }
            else if (c < 0x10000) { out[n++] = 0xe0 | c >> 12; out[n++] = 0x80 | c >> 6 & 63; out[n++] = 0x80 | c & 63; }
            else { out[n++] = 0xf0 | c >> 18; out[n++] = 0x80 | c >> 12 & 63; out[n++] = 0x80 | c >> 6 & 63; out[n++] = 0x80 | c & 63; }
        }
        return out.subarray(0, n);
    }

    function decodeText(bytes) {
        var s = "", i = 0, n = bytes.length;
        while (i < n) {
            var c = bytes[i++], cp;
            if (c < 0x80) cp = c;
            else if (c >= 0xc0 && c < 0xe0 && i < n) cp = (c & 31) << 6 | bytes[i++] & 63;
            else if (c >= 0xe0 && c < 0xf0 && i + 1 < n) { cp = (c & 15) << 12 | (bytes[i] & 63) << 6 | bytes[i + 1] & 63; i += 2; }
            else if (c >= 0xf0 && i + 2 < n) { cp = (c & 7) << 18 | (bytes[i] & 63) << 12 | (bytes[i + 1] & 63) << 6 | bytes[i + 2] & 63; i += 3; }
            else cp = 0xfffd;
            if (cp >= 0x10000) { cp -= 0x10000; s += String.fromCharCode(0xd800 + (cp >> 10), 0xdc00 + (cp & 1023)); }
            else s += String.fromCharCode(cp);
        }
        return s;
    }

    // Returns the full request as bytes. body: string | Uint8Array | null.
    function buildRequest(options) {
        var body = options.body == null ? null : (typeof options.body === "string" ? encodeText(options.body) : options.body);
        // Host carries the server name (reverse proxies route on it); the port is
        // omitted only when it is the scheme's default.
        var defaultPort = options.tls ? 443 : 80;
        var headers = {Host: options.host + (options.port && options.port !== defaultPort ? ":" + options.port : ""), Connection: "close", Accept: "*/*"};
        var given = options.headers || {};
        for (var k in given) if (Object.prototype.hasOwnProperty.call(given, k)) headers[k] = given[k];
        if (body) headers["Content-Length"] = String(body.length);
        if (/[\r\n]/.test(options.path) || !/^\/[^ ]*$/.test(options.path)) throw new Error("Invalid request path");
        var head = (options.method || "GET") + " " + options.path + " HTTP/1.1\r\n";
        for (var name in headers) {
            if (/[\r\n:]/.test(name) || /[\r\n]/.test(String(headers[name]))) throw new Error("Invalid header");
            head += name + ": " + headers[name] + "\r\n";
        }
        var headBytes = encodeText(head + "\r\n");
        if (!body) return headBytes;
        var out = new Uint8Array(headBytes.length + body.length);
        out.set(headBytes, 0); out.set(body, headBytes.length);
        return out;
    }

    // Incremental response parser. Callbacks: onHead({status, reason, headers}), onBody(Uint8Array), onEnd().
    function createParser(cb) {
        var state = "head", buffer = new Uint8Array(0), remaining = 0, chunkRemaining = 0, ended = false, head = null, noBody = false;
        function append(chunk) {
            if (!buffer.length) { buffer = chunk; return; }
            var merged = new Uint8Array(buffer.length + chunk.length);
            merged.set(buffer, 0); merged.set(chunk, buffer.length); buffer = merged;
        }
        function find(crlf2) {
            for (var i = 0; i + 1 < buffer.length; i++) {
                if (crlf2) { if (i + 3 < buffer.length && buffer[i] === 13 && buffer[i + 1] === 10 && buffer[i + 2] === 13 && buffer[i + 3] === 10) return i; }
                else if (buffer[i] === 13 && buffer[i + 1] === 10) return i;
            }
            return -1;
        }
        function finish() { if (!ended) { ended = true; state = "done"; if (cb.onEnd) cb.onEnd(); } }
        function parseHead(text) {
            var lines = text.split("\r\n"), m = /^HTTP\/1\.[01] (\d{3})(?: (.*))?$/.exec(lines[0]);
            if (!m) throw new Error("Invalid HTTP status line");
            var headers = {};
            for (var i = 1; i < lines.length; i++) {
                var at = lines[i].indexOf(":");
                if (at < 1) throw new Error("Invalid HTTP header");
                var name = lines[i].slice(0, at).trim().toLowerCase(), value = lines[i].slice(at + 1).trim();
                headers[name] = headers[name] === undefined ? value : headers[name] + ", " + value;
            }
            return {status: Number(m[1]), reason: m[2] || "", headers: headers};
        }
        function drain() {
            for (;;) {
                if (state === "head") {
                    var end = find(true);
                    if (end < 0) { if (buffer.length > 65536) throw new Error("HTTP headers too large"); return; }
                    head = parseHead(decodeText(buffer.subarray(0, end)));
                    buffer = buffer.subarray(end + 4);
                    if (head.status >= 100 && head.status < 200 && head.status !== 101) { head = null; continue; } // interim response
                    if (cb.onHead) cb.onHead(head);
                    noBody = head.status === 204 || head.status === 304 || cb.noBody === true;
                    var te = head.headers["transfer-encoding"], cl = head.headers["content-length"];
                    if (noBody) { finish(); return; }
                    if (te && /chunked/i.test(te)) state = "chunk-size";
                    else if (cl !== undefined) {
                        if (!/^\d+$/.test(cl)) throw new Error("Invalid content length");
                        remaining = Number(cl); state = "length";
                        if (!remaining) { finish(); return; }
                    } else state = "close";
                } else if (state === "length") {
                    if (!buffer.length) return;
                    var take = Math.min(remaining, buffer.length);
                    if (cb.onBody) cb.onBody(buffer.subarray(0, take));
                    buffer = buffer.subarray(take); remaining -= take;
                    if (!remaining) { finish(); return; }
                } else if (state === "close") {
                    if (!buffer.length) return;
                    if (cb.onBody) cb.onBody(buffer); buffer = new Uint8Array(0); return;
                } else if (state === "chunk-size") {
                    var eol = find(false);
                    if (eol < 0) { if (buffer.length > 1024) throw new Error("Invalid chunk header"); return; }
                    var line = decodeText(buffer.subarray(0, eol)).split(";")[0].trim();
                    if (!/^[0-9a-fA-F]+$/.test(line)) throw new Error("Invalid chunk size");
                    chunkRemaining = parseInt(line, 16); buffer = buffer.subarray(eol + 2);
                    state = chunkRemaining ? "chunk-data" : "trailers";
                } else if (state === "chunk-data") {
                    if (!buffer.length) return;
                    var n = Math.min(chunkRemaining, buffer.length);
                    if (cb.onBody) cb.onBody(buffer.subarray(0, n));
                    buffer = buffer.subarray(n); chunkRemaining -= n;
                    if (!chunkRemaining) state = "chunk-end";
                } else if (state === "chunk-end") {
                    if (buffer.length < 2) return;
                    if (buffer[0] !== 13 || buffer[1] !== 10) throw new Error("Invalid chunk terminator");
                    buffer = buffer.subarray(2); state = "chunk-size";
                } else if (state === "trailers") {
                    var t = find(false);
                    if (t < 0) { if (buffer.length > 8192) throw new Error("HTTP trailers too large"); return; }
                    var empty = t === 0; buffer = buffer.subarray(t + 2);
                    if (empty) { finish(); return; }
                } else return;
            }
        }
        return {
            push: function (chunk) { if (ended || !chunk.length) return; append(chunk); drain(); },
            // Connection closed by the peer.
            eof: function () {
                if (ended) return;
                if (state === "close") finish();
                else throw new Error("Connection closed before the response completed");
            },
            done: function () { return ended; }
        };
    }

    // Collects a whole response. transport.request(options, handlers) -> Promise.
    async function fetchBytes(transport, options) {
        var parts = [], total = 0, info = null, limit = options.maxBytes || 8 * 1048576;
        await transport.request(options, {
            onHead: function (head) {
                info = head;
                var cl = Number(head.headers["content-length"]);
                if (isFinite(cl) && cl > limit) throw new Error("Response larger than " + limit + " bytes");
            },
            onBody: function (chunk) {
                total += chunk.length;
                if (total > limit) throw new Error("Response larger than " + limit + " bytes");
                parts.push(chunk.slice());
            }
        });
        var body = new Uint8Array(total), at = 0;
        for (var i = 0; i < parts.length; i++) { body.set(parts[i], at); at += parts[i].length; }
        return {status: info.status, headers: info.headers, body: body};
    }

    async function fetchJson(transport, options) {
        var res = await fetchBytes(transport, options);
        var text = decodeText(res.body), data = null;
        if (text) { try { data = JSON.parse(text); } catch (e) { if (res.status < 400) throw new Error("Server returned invalid JSON"); } }
        return {status: res.status, headers: res.headers, data: data, text: text};
    }

    var B64 = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    function toBase64(bytes) {
        var out = "", n = bytes.length, i = 0;
        for (; i + 2 < n; i += 3) {
            var v = bytes[i] << 16 | bytes[i + 1] << 8 | bytes[i + 2];
            out += B64[v >> 18 & 63] + B64[v >> 12 & 63] + B64[v >> 6 & 63] + B64[v & 63];
        }
        if (i < n) {
            var w = bytes[i] << 16 | (i + 1 < n ? bytes[i + 1] << 8 : 0);
            out += B64[w >> 18 & 63] + B64[w >> 12 & 63] + (i + 1 < n ? B64[w >> 6 & 63] : "=") + "=";
        }
        return out;
    }

    // data: URL for binary content. Uses the engine's btoa when present (as
    // Moonlight does), building the binary string in chunks to limit garbage.
    function toDataUrl(bytes, mime) {
        if (typeof G.btoa === "function") {
            var parts = [];
            for (var i = 0; i < bytes.length; i += 8192) parts.push(String.fromCharCode.apply(null, bytes.subarray(i, i + 8192)));
            return "data:" + mime + ";base64," + G.btoa(parts.join(""));
        }
        return "data:" + mime + ";base64," + toBase64(bytes);
    }

    JF.http = {toDataUrl: toDataUrl, encodeText: encodeText, decodeText: decodeText, buildRequest: buildRequest, createParser: createParser,
        fetchBytes: fetchBytes, fetchJson: fetchJson, toBase64: toBase64};
    if (typeof module !== "undefined" && module.exports) module.exports = JF.http;
})();

;
/* Minimal DNS client codec (RFC 1035): builds A-record queries and parses
 * responses, including name compression and CNAME chains. Pure logic; the
 * platform sends the bytes over UDP. */
(function () {
    "use strict";
    var G = typeof window !== "undefined" ? window : globalThis;
    var JF = G.JF = G.JF || {};

    function validName(name) {
        if (!name || name.length > 253) return false;
        var labels = name.replace(/\.$/, "").split(".");
        if (labels.length < 1) return false;
        for (var i = 0; i < labels.length; i++) {
            if (!/^[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?$/.test(labels[i])) return false;
        }
        return true;
    }

    // Query for name (type A, class IN, recursion desired).
    function buildQuery(id, name) {
        if (!validName(name)) throw new Error("Invalid server name: " + name);
        var labels = name.replace(/\.$/, "").split("."), size = 12 + 5;
        labels.forEach(function (l) { size += l.length + 1; });
        var out = new Uint8Array(size), o = 0;
        out[o++] = id >> 8 & 255; out[o++] = id & 255;
        out[o++] = 0x01; out[o++] = 0x00;            // RD
        out[o++] = 0; out[o++] = 1;                  // QDCOUNT 1
        o += 6;                                      // AN, NS, AR = 0
        labels.forEach(function (l) {
            out[o++] = l.length;
            for (var i = 0; i < l.length; i++) out[o++] = l.toLowerCase().charCodeAt(i);
        });
        out[o++] = 0;
        out[o++] = 0; out[o++] = 1;                  // QTYPE A
        out[o++] = 0; out[o++] = 1;                  // QCLASS IN
        return out;
    }

    // Reads a (possibly compressed) name at offset. Returns {name, next}.
    function readName(b, offset) {
        var labels = [], jumped = false, next = offset, hops = 0;
        for (;;) {
            if (offset >= b.length) throw new Error("DNS name runs past the message");
            var len = b[offset];
            if ((len & 0xc0) === 0xc0) {
                if (offset + 1 >= b.length) throw new Error("Truncated DNS pointer");
                if (++hops > 16) throw new Error("DNS compression loop");
                var ptr = ((len & 0x3f) << 8) | b[offset + 1];
                if (!jumped) next = offset + 2;
                jumped = true; offset = ptr; continue;
            }
            if (len & 0xc0) throw new Error("Unsupported DNS label type");
            if (len === 0) { if (!jumped) next = offset + 1; break; }
            if (offset + 1 + len > b.length) throw new Error("Truncated DNS label");
            var label = "";
            for (var i = 1; i <= len; i++) label += String.fromCharCode(b[offset + i]);
            labels.push(label); offset += 1 + len;
        }
        return {name: labels.join(".").toLowerCase(), next: next};
    }

    var RCODES = {1: "format error", 2: "server failure", 3: "name does not exist", 4: "not implemented", 5: "refused"};

    // Returns {addresses: ["1.2.3.4", ...], ttl, cname} or throws with a readable reason.
    function parseResponse(b, id, name) {
        if (b.length < 12) throw new Error("DNS response too short");
        if ((b[0] << 8 | b[1]) !== id) throw new Error("DNS response for a different query");
        if (!(b[2] & 0x80)) throw new Error("Not a DNS response");
        if (b[2] & 0x02) throw new Error("DNS response truncated");
        var rcode = b[3] & 0x0f;
        if (rcode) { var e = new Error("DNS: " + (RCODES[rcode] || "error " + rcode)); e.rcode = rcode; throw e; }
        var qd = b[4] << 8 | b[5], an = b[6] << 8 | b[7], o = 12;
        for (var q = 0; q < qd; q++) { o = readName(b, o).next + 4; }
        var target = String(name).replace(/\.$/, "").toLowerCase(), cname = null, records = [];
        for (var a = 0; a < an; a++) {
            var owner = readName(b, o); o = owner.next;
            if (o + 10 > b.length) throw new Error("Truncated DNS record");
            var type = b[o] << 8 | b[o + 1], klass = b[o + 2] << 8 | b[o + 3];
            var ttl = ((b[o + 4] << 24) >>> 0) + (b[o + 5] << 16) + (b[o + 6] << 8) + b[o + 7];
            var rdlength = b[o + 8] << 8 | b[o + 9]; o += 10;
            if (o + rdlength > b.length) throw new Error("Truncated DNS record data");
            records.push({owner: owner.name, type: type, klass: klass, ttl: ttl, start: o, length: rdlength});
            o += rdlength;
        }
        // Follow CNAMEs from the queried name to its A records.
        var current = target, ttlMin = Infinity;
        for (var hop = 0; hop < 8; hop++) {
            var found = records.filter(function (r) { return r.owner === current && r.klass === 1; });
            var aRecords = found.filter(function (r) { return r.type === 1 && r.length === 4; });
            if (aRecords.length) {
                return {
                    addresses: aRecords.map(function (r) { return b[r.start] + "." + b[r.start + 1] + "." + b[r.start + 2] + "." + b[r.start + 3]; }),
                    ttl: Math.min(ttlMin, aRecords.reduce(function (m, r) { return Math.min(m, r.ttl); }, Infinity)),
                    cname: cname
                };
            }
            var alias = found.filter(function (r) { return r.type === 5; })[0];
            if (!alias) break;
            ttlMin = Math.min(ttlMin, alias.ttl);
            current = cname = readName(b, alias.start).name;
        }
        throw new Error("DNS: no IPv4 address for " + target);
    }

    JF.dns = {buildQuery: buildQuery, parseResponse: parseResponse, validName: validName};
    if (typeof module !== "undefined" && module.exports) module.exports = JF.dns;
})();

;
/* HLS media playlist parsing and URL resolution. Pure logic. */
(function () {
    "use strict";
    var G = typeof window !== "undefined" ? window : globalThis;
    var JF = G.JF = G.JF || {};

    // Returns {targetDuration, endList, segments:[{uri, duration, start}], totalDuration}.
    function parsePlaylist(text) {
        var lines = String(text).split(/\r?\n/), result = {targetDuration: 0, endList: false, mediaSequence: 0, segments: [], totalDuration: 0, encrypted: false, fmp4: false};
        if (!lines.length || lines[0].trim() !== "#EXTM3U") throw new Error("Not an HLS playlist");
        var pending = null, start = 0;
        for (var i = 1; i < lines.length; i++) {
            var line = lines[i].trim();
            if (!line) continue;
            if (line.indexOf("#EXT-X-TARGETDURATION:") === 0) result.targetDuration = Number(line.slice(22)) || 0;
            else if (line.indexOf("#EXT-X-MEDIA-SEQUENCE:") === 0) result.mediaSequence = Number(line.slice(22)) || 0;
            else if (line === "#EXT-X-ENDLIST") result.endList = true;
            else if (line.indexOf("#EXT-X-KEY:") === 0 && !/METHOD=NONE/.test(line)) result.encrypted = true;
            else if (line.indexOf("#EXT-X-MAP:") === 0) result.fmp4 = true;
            else if (line.indexOf("#EXTINF:") === 0) pending = Number(line.slice(8).split(",")[0]);
            else if (line[0] !== "#") {
                if (pending === null || !isFinite(pending)) throw new Error("Segment without duration");
                result.segments.push({uri: line, duration: pending, start: start});
                start += pending; pending = null;
            }
        }
        result.totalDuration = start;
        return result;
    }

    // Master playlists list variants; pick the first (Jellyfin offers one per request).
    function firstVariant(text) {
        var lines = String(text).split(/\r?\n/);
        for (var i = 0; i < lines.length; i++) {
            if (lines[i].indexOf("#EXT-X-STREAM-INF:") === 0) {
                for (var j = i + 1; j < lines.length; j++) if (lines[j].trim() && lines[j][0] !== "#") return lines[j].trim();
            }
        }
        return null;
    }

    // Resolve a playlist entry against the playlist's own path (server-relative paths only).
    function resolve(basePath, uri) {
        if (/^https?:\/\//i.test(uri)) {
            var m = /^https?:\/\/[^\/]+(\/.*)?$/i.exec(uri);
            return m && m[1] ? m[1] : "/";
        }
        if (uri[0] === "/") return uri;
        var queryAt = basePath.indexOf("?"), path = queryAt >= 0 ? basePath.slice(0, queryAt) : basePath;
        var dir = path.slice(0, path.lastIndexOf("/") + 1), parts = (dir + uri).split("?"), segs = parts[0].split("/"), out = [];
        for (var i = 0; i < segs.length; i++) {
            if (segs[i] === "..") out.pop(); else if (segs[i] !== ".") out.push(segs[i]);
        }
        return out.join("/") + (parts.length > 1 ? "?" + parts.slice(1).join("?") : "");
    }

    // Index of the segment containing `seconds`.
    function segmentAt(playlist, seconds) {
        var segments = playlist.segments;
        for (var i = segments.length - 1; i >= 0; i--) if (segments[i].start <= seconds) return i;
        return 0;
    }

    JF.hls = {parsePlaylist: parsePlaylist, firstVariant: firstVariant, resolve: resolve, segmentAt: segmentAt};
    if (typeof module !== "undefined" && module.exports) module.exports = JF.hls;
})();

;
/* H.264 sequence parameter set parser: coded and cropped dimensions. Pure logic. */
(function () {
    "use strict";
    var G = typeof window !== "undefined" ? window : globalThis;
    var JF = G.JF = G.JF || {};

    function unescape(nal) {
        var out = new Uint8Array(nal.length), n = 0, zeros = 0;
        for (var i = 0; i < nal.length; i++) {
            if (zeros >= 2 && nal[i] === 3) { zeros = 0; continue; }   // emulation prevention byte
            out[n++] = nal[i]; zeros = nal[i] === 0 ? zeros + 1 : 0;
        }
        return out.subarray(0, n);
    }

    function Bits(bytes) { this.b = bytes; this.p = 0; }
    Bits.prototype.u = function (n) {
        var v = 0;
        for (var i = 0; i < n; i++) {
            var byte = this.b[this.p >> 3];
            if (byte === undefined) throw new Error("SPS truncated");
            v = v * 2 + ((byte >> (7 - (this.p & 7))) & 1); this.p++;
        }
        return v;
    };
    Bits.prototype.ue = function () {
        var zeros = 0;
        while (this.u(1) === 0) { if (++zeros > 32) throw new Error("Invalid SPS exp-golomb"); }
        return zeros ? Math.pow(2, zeros) - 1 + this.u(zeros) : 0;
    };
    Bits.prototype.se = function () { var k = this.ue(); return (k & 1) ? (k + 1) / 2 : -(k / 2); };

    function skipScalingList(bits, size) {
        var last = 8, next = 8;
        for (var j = 0; j < size; j++) {
            if (next !== 0) next = (last + bits.se() + 256) % 256;
            last = next === 0 ? last : next;
        }
    }

    // nal: the SPS NAL unit including its one-byte header, without a start code.
    function parse(nal) {
        if (!nal || nal.length < 4 || (nal[0] & 0x1f) !== 7) throw new Error("Not an SPS NAL unit");
        var bits = new Bits(unescape(nal.subarray(1)));
        var profile = bits.u(8); bits.u(8); var level = bits.u(8);
        bits.ue();                                                   // seq_parameter_set_id
        var chroma = 1;
        if ([100, 110, 122, 244, 44, 83, 86, 118, 128, 138, 139, 134, 135].indexOf(profile) >= 0) {
            chroma = bits.ue();
            if (chroma === 3) bits.u(1);
            bits.ue(); bits.ue(); bits.u(1);
            if (bits.u(1)) for (var i = 0; i < (chroma !== 3 ? 8 : 12); i++) if (bits.u(1)) skipScalingList(bits, i < 6 ? 16 : 64);
        }
        bits.ue();                                                   // log2_max_frame_num_minus4
        var pocType = bits.ue();
        if (pocType === 0) bits.ue();
        else if (pocType === 1) { bits.u(1); bits.se(); bits.se(); var cycle = bits.ue(); for (var c = 0; c < cycle; c++) bits.se(); }
        var refFrames = bits.ue(); bits.u(1);
        var mbsWide = bits.ue() + 1, mapUnitsHigh = bits.ue() + 1, frameMbsOnly = bits.u(1);
        if (!frameMbsOnly) bits.u(1);
        bits.u(1);
        var cropLeft = 0, cropRight = 0, cropTop = 0, cropBottom = 0;
        if (bits.u(1)) { cropLeft = bits.ue(); cropRight = bits.ue(); cropTop = bits.ue(); cropBottom = bits.ue(); }
        var unitX = chroma === 0 ? 1 : (chroma === 3 ? 1 : 2);
        var unitY = (chroma === 0 || chroma === 1 ? (chroma === 0 ? 1 : 2) : 1) * (frameMbsOnly ? 1 : 2);
        var codedWidth = mbsWide * 16, codedHeight = (frameMbsOnly ? 1 : 2) * mapUnitsHigh * 16;
        return {
            profile: profile, level: level, chromaFormat: chroma, refFrames: refFrames,
            codedWidth: codedWidth, codedHeight: codedHeight,
            width: codedWidth - unitX * (cropLeft + cropRight),
            height: codedHeight - unitY * (cropTop + cropBottom)
        };
    }

    JF.h264Sps = {parse: parse};
    if (typeof module !== "undefined" && module.exports) module.exports = JF.h264Sps;
})();

;
/* MPEG-TS demuxer: H.264 (Annex-B access units) and Opus (raw packets).
 * Pure logic, no platform dependencies. Timestamps are 90 kHz ticks. */
(function () {
    "use strict";
    var G = typeof window !== "undefined" ? window : globalThis;
    var JF = G.JF = G.JF || {};
    var PACKET = 188;

    // Samples (at 48 kHz) in one Opus packet, from its TOC byte(s).
    function opusSamples(data) {
        if (!data || data.length < 1) return 0;
        var toc = data[0], config = toc >> 3, frame;
        if (config < 12) frame = [480, 960, 1920, 2880][config & 3];
        else if (config < 16) frame = [480, 960][config & 1];
        else frame = [120, 240, 480, 960][config & 3];
        var code = toc & 3, count;
        if (code === 0) count = 1;
        else if (code === 1 || code === 2) count = 2;
        else { if (data.length < 2) return 0; count = data[1] & 0x3f; }
        return frame * count;
    }

    function readTimestamp(b, o) {
        // 33-bit value split across 5 bytes; use arithmetic, not 32-bit shifts.
        return ((b[o] >> 1) & 7) * 1073741824 + (b[o + 1] << 22 | (b[o + 2] >> 1) << 15 | b[o + 3] << 7 | b[o + 4] >> 1) * 1;
    }

    function concat(parts, total) {
        if (parts.length === 1) return parts[0];
        var out = new Uint8Array(total), at = 0;
        for (var i = 0; i < parts.length; i++) { out.set(parts[i], at); at += parts[i].length; }
        return out;
    }

    // Walks Annex-B start codes. Calls visit(type, start, end) for each NAL
    // (start/end exclude the start code).
    function eachNal(data, visit) {
        var n = data.length, i = 0, start = -1;
        while (i + 2 < n) {
            if (data[i] === 0 && data[i + 1] === 0 && data[i + 2] === 1) {
                if (start >= 0) {
                    var end = i; while (end > start && data[end - 1] === 0) end--;
                    visit(data[start] & 0x1f, start, end);
                }
                start = i + 3; i += 3;
            } else i++;
        }
        if (start >= 0 && start < n) visit(data[start] & 0x1f, start, n);
    }

    function create(callbacks) {
        callbacks = callbacks || {};
        var carry = null, pmtPid = -1, videoPid = -1, audioPid = -1, unsupportedAudio = null;
        var streams = {};            // pid -> {kind, parts, total, expected, pts, dts}
        var stats = {packets: 0, resyncs: 0, video: 0, audio: 0};
        var sections = {};           // pid -> {parts, total} for PAT/PMT reassembly

        function emitVideo(stream) {
            var data = concat(stream.parts, stream.total);
            if (!data.length) return;
            var keyframe = false, sps = null, pps = null;
            eachNal(data, function (type, s, e) {
                if (type === 5) keyframe = true;
                else if (type === 7 && !sps) sps = data.subarray(s, e);
                else if (type === 8 && !pps) pps = data.subarray(s, e);
            });
            stats.video++;
            if (callbacks.onVideo) callbacks.onVideo({
                pts: stream.pts, dts: stream.dts === null ? stream.pts : stream.dts,
                data: data, keyframe: keyframe, sps: sps, pps: pps
            });
        }

        function emitAudio(stream) {
            var b = concat(stream.parts, stream.total), pos = 0, ts = stream.pts;
            while (pos + 3 <= b.length) {
                if (b[pos] !== 0x7f || (b[pos + 1] & 0xe0) !== 0xe0) break; // not an Opus control header
                var flags = b[pos + 1]; pos += 2;
                var size = 0, v;
                do { if (pos >= b.length) return; v = b[pos++]; size += v; } while (v === 255);
                if (flags & 0x10) pos += 2;           // start trim
                if (flags & 0x08) pos += 2;           // end trim
                if (flags & 0x04) { if (pos >= b.length) return; pos += 1 + b[pos]; } // control extension
                if (size < 1 || pos + size > b.length) return;
                var packet = b.subarray(pos, pos + size); pos += size;
                var samples = opusSamples(packet);
                stats.audio++;
                if (callbacks.onAudio) callbacks.onAudio({pts: ts, data: packet, samples: samples});
                ts += samples * 90000 / 48000;
            }
        }

        function flush(stream) {
            if (!stream || !stream.total) { if (stream) { stream.parts = []; stream.total = 0; } return; }
            try { if (stream.kind === "video") emitVideo(stream); else if (stream.kind === "audio") emitAudio(stream); }
            finally { stream.parts = []; stream.total = 0; stream.expected = 0; }
        }

        function parsePes(stream, payload) {
            // payload begins at the PES start code.
            if (payload.length < 9 || payload[0] !== 0 || payload[1] !== 0 || payload[2] !== 1) return null;
            var length = (payload[4] << 8) | payload[5], flags = payload[7], headerLength = payload[8];
            if (payload.length < 9 + headerLength) return null;
            stream.pts = null; stream.dts = null;
            if (flags & 0x80) stream.pts = readTimestamp(payload, 9);
            if (flags & 0x40) stream.dts = readTimestamp(payload, 14);
            stream.expected = length ? length + 6 : 0;
            return payload.subarray(9 + headerLength);
        }

        function addPes(pid, pusi, payload) {
            var stream = streams[pid];
            if (!stream) return;
            if (pusi) {
                flush(stream);
                var body = parsePes(stream, payload);
                if (!body) return;
                stream.parts = [body]; stream.total = body.length; stream.received = payload.length;
            } else {
                if (!stream.total && !stream.received) return;
                stream.parts.push(payload); stream.total += payload.length; stream.received += payload.length;
            }
            if (stream.expected && stream.received >= stream.expected) { flush(stream); stream.received = 0; }
        }

        function parseSection(pid, pusi, payload, handler) {
            var sec = sections[pid];
            if (pusi) {
                var pointer = payload[0]; payload = payload.subarray(1 + pointer);
                sec = sections[pid] = {parts: [], total: 0, need: -1};
            }
            if (!sec) return;
            sec.parts.push(payload); sec.total += payload.length;
            var data = concat(sec.parts, sec.total);
            if (sec.need < 0 && data.length >= 3) sec.need = 3 + (((data[1] & 0x0f) << 8) | data[2]);
            if (sec.need > 0 && data.length >= sec.need) { sections[pid] = null; handler(data.subarray(0, sec.need)); }
        }

        function onPat(section) {
            var end = section.length - 4;
            for (var o = 8; o + 4 <= end; o += 4) {
                var program = (section[o] << 8) | section[o + 1];
                if (program !== 0) { pmtPid = ((section[o + 2] & 0x1f) << 8) | section[o + 3]; return; }
            }
        }

        function onPmt(section) {
            var end = section.length - 4;
            var infoLength = ((section[10] & 0x0f) << 8) | section[11];
            var o = 12 + infoLength;
            while (o + 5 <= end) {
                var type = section[o], pid = ((section[o + 1] & 0x1f) << 8) | section[o + 2];
                var esLength = ((section[o + 3] & 0x0f) << 8) | section[o + 4];
                var isOpus = false;
                for (var d = o + 5; d + 2 <= o + 5 + esLength; ) {
                    var tag = section[d], len = section[d + 1];
                    if (tag === 0x05 && len >= 4 && section[d + 2] === 0x4f && section[d + 3] === 0x70 &&
                        section[d + 4] === 0x75 && section[d + 5] === 0x73) isOpus = true; // "Opus"
                    d += 2 + len;
                }
                if (type === 0x1b && videoPid < 0) {
                    videoPid = pid; streams[pid] = {kind: "video", parts: [], total: 0, expected: 0, received: 0, pts: null, dts: null};
                } else if (type === 0x06 && isOpus && audioPid < 0) {
                    audioPid = pid; streams[pid] = {kind: "audio", parts: [], total: 0, expected: 0, received: 0, pts: null, dts: null};
                } else if ((type === 0x0f || type === 0x11 || type === 0x03 || type === 0x04 || type === 0x81 || type === 0x87 || type === 0x06) && !isOpus && !unsupportedAudio) {
                    unsupportedAudio = {pid: pid, streamType: type};
                    if (callbacks.onUnsupportedAudio) callbacks.onUnsupportedAudio(unsupportedAudio);
                }
                o += 5 + esLength;
            }
            if (callbacks.onProgram) callbacks.onProgram({videoPid: videoPid, audioPid: audioPid});
        }

        function packet(b, o) {
            var pid = ((b[o + 1] & 0x1f) << 8) | b[o + 2];
            var pusi = (b[o + 1] & 0x40) !== 0, afc = (b[o + 3] >> 4) & 3;
            if (!(afc & 1)) return;                       // no payload
            var start = o + 4;
            if (afc & 2) start += 1 + b[o + 4];
            if (start >= o + PACKET) return;
            var payload = b.subarray(start, o + PACKET);
            stats.packets++;
            if (pid === 0) parseSection(0, pusi, payload, onPat);
            else if (pid === pmtPid) parseSection(pid, pusi, payload, onPmt);
            else if (streams[pid]) addPes(pid, pusi, payload);
        }

        function push(chunk) {
            var b = chunk;
            if (carry && carry.length) {
                b = new Uint8Array(carry.length + chunk.length);
                b.set(carry, 0); b.set(chunk, carry.length);
            }
            var o = 0, n = b.length;
            while (o + PACKET <= n) {
                if (b[o] !== 0x47) {                       // resynchronize on the next confirmed sync byte
                    var next = -1;
                    for (var s = o + 1; s + PACKET < n; s++) if (b[s] === 0x47 && b[s + PACKET] === 0x47) { next = s; break; }
                    stats.resyncs++;
                    if (next < 0) { o = n; break; }
                    o = next; continue;
                }
                packet(b, o); o += PACKET;
            }
            carry = o < n ? b.slice(o) : null;
        }

        function end() { for (var pid in streams) flush(streams[pid]); }
        return {push: push, end: end, stats: stats, program: function () { return {videoPid: videoPid, audioPid: audioPid, unsupportedAudio: unsupportedAudio}; }};
    }

    JF.tsDemux = {create: create, opusSamples: opusSamples, eachNal: eachNal};
    if (typeof module !== "undefined" && module.exports) module.exports = JF.tsDemux;
})();

;
/* Jellyfin REST client. Routes and field names checked against the Jellyfin
 * server source (Jellyfin.Api controllers). Platform-independent: all I/O goes
 * through an injected transport (see http.js). */
/* Credits: endpoints, parameters and DTO field names were checked against the
 * Jellyfin server source (https://github.com/jellyfin/jellyfin). This is an
 * independent, unofficial client. */
(function () {
    "use strict";
    var G = typeof window !== "undefined" ? window : globalThis;
    var JF = G.JF = G.JF || {};
    var TICKS = 10000000; // Jellyfin ticks per second

    function query(params) {
        var parts = [];
        for (var k in params) {
            if (!Object.prototype.hasOwnProperty.call(params, k)) continue;
            var v = params[k];
            if (v === undefined || v === null || v === "") continue;
            parts.push(encodeURIComponent(k) + "=" + encodeURIComponent(String(v)));
        }
        return parts.length ? "?" + parts.join("&") : "";
    }

    function quote(value) { return '"' + String(value).replace(/["\\\r\n]/g, "") + '"'; }

    function randomHex(n) {
        var s = "";
        for (var i = 0; i < n; i++) s += Math.floor(Math.random() * 16).toString(16);
        return s;
    }

    function create(options) {
        var http = JF.http;
        var transport = options.transport;
        var server = {host: options.host, port: options.port || 8096, tls: !!options.tls, basePath: options.basePath || ""};
        // Server clock: each reply's Date header gives the offset to the console's clock.
        var clockOffset = 0, clockSynced = false;
        function noteServerTime(headers) {
            var t = headers && headers.date ? Date.parse(headers.date) : NaN;
            if (!isFinite(t)) return;
            // Date has 1 s resolution; the true time is within [t, t + 1000).
            clockOffset = t + 500 - Date.now(); clockSynced = true;
        }
        function full(path) { return server.basePath + path; }
        var session = {token: options.token || null, userId: options.userId || null};
        var device = {
            client: options.client || "YAJA",
            name: options.deviceName || "Nintendo Switch",
            id: options.deviceId || ("civswitch-" + randomHex(16)),
            version: options.version || "0.1.0"
        };

        function authorization() {
            var value = "MediaBrowser Client=" + quote(device.client) + ", Device=" + quote(device.name) +
                ", DeviceId=" + quote(device.id) + ", Version=" + quote(device.version);
            if (session.token) value += ", Token=" + quote(session.token);
            return value;
        }

        function headers(extra) {
            var h = {Authorization: authorization(), Accept: "application/json"};
            for (var k in extra || {}) h[k] = extra[k];
            return h;
        }

        function failure(res, path) {
            var message = "Server error " + res.status;
            if (res.status === 401) message = "Your session has expired or the login was rejected.";
            else if (res.status === 403) message = "This account is not allowed to do that.";
            else if (res.status === 404) message = "Not found on the server.";
            else if (res.data && typeof res.data === "object" && (res.data.title || res.data.Message)) message += ": " + (res.data.title || res.data.Message);
            var error = new Error(message);
            error.status = res.status; error.path = path;
            return error;
        }

        async function call(method, path, params, body, opts) {
            opts = opts || {};
            var target = full(path) + query(params || {});
            var res = await http.fetchJson(transport, {
                host: server.host, port: server.port, tls: server.tls, method: method, path: target,
                headers: headers(body !== undefined ? {"Content-Type": "application/json"} : null),
                body: body !== undefined ? JSON.stringify(body) : null,
                timeoutMs: opts.timeoutMs || 15000, maxBytes: opts.maxBytes || 4 * 1048576, signal: opts.signal
            });
            noteServerTime(res.headers);
            if (res.status >= 400) throw failure(res, target);
            return res.data;
        }

        function requireUser() { if (!session.userId || !session.token) throw new Error("Not signed in."); }

        var itemFields = "Overview,PrimaryImageAspectRatio,MediaSources,ProductionYear,RunTimeTicks,UserData,SortName";

        var api = {
            device: device,
            server: server,
            session: function () { return {token: session.token, userId: session.userId}; },
            setSession: function (token, userId) { session.token = token; session.userId = userId; },
            // Current time by the server's clock (falls back to the console's).
            now: function () { return Date.now() + clockOffset; },
            clockSynced: function () { return clockSynced; },
            authorization: authorization,
            ticks: TICKS,

            publicInfo: function () { return call("GET", "/System/Info/Public", null, undefined, {timeoutMs: 6000}); },

            login: async function (username, password) {
                var result = await call("POST", "/Users/AuthenticateByName", null, {Username: username, Pw: password || ""});
                if (!result || !result.AccessToken || !result.User) throw new Error("Unexpected login response.");
                session.token = result.AccessToken; session.userId = result.User.Id;
                return {token: session.token, userId: session.userId, userName: result.User.Name};
            },

            quickConnectEnabled: async function () { try { return (await call("GET", "/QuickConnect/Enabled")) === true; } catch (e) { return false; } },
            quickConnectStart: function () { return call("POST", "/QuickConnect/Initiate"); },
            quickConnectState: function (secret) { return call("GET", "/QuickConnect/Connect", {secret: secret}); },
            quickConnectFinish: async function (secret) {
                var result = await call("POST", "/Users/AuthenticateWithQuickConnect", null, {Secret: secret});
                if (!result || !result.AccessToken || !result.User) throw new Error("Unexpected Quick Connect response.");
                session.token = result.AccessToken; session.userId = result.User.Id;
                return {token: session.token, userId: session.userId, userName: result.User.Name};
            },

            currentUser: function () { requireUser(); return call("GET", "/Users/Me"); },

            views: async function () { requireUser(); var r = await call("GET", "/UserViews", {userId: session.userId}); return (r && r.Items) || []; },

            items: async function (params) {
                requireUser();
                var p = {userId: session.userId, fields: itemFields, enableImageTypes: "Primary", imageTypeLimit: 1, enableTotalRecordCount: true};
                for (var k in params || {}) p[k] = params[k];
                var r = await call("GET", "/Items", p);
                return {items: (r && r.Items) || [], total: (r && r.TotalRecordCount) || 0};
            },

            library: function (parentId, startIndex, limit) {
                return api.items({parentId: parentId, recursive: true, includeItemTypes: "Movie,Series", sortBy: "SortName,ProductionYear", sortOrder: "Ascending", startIndex: startIndex || 0, limit: limit || 48});
            },

            resume: async function () {
                requireUser();
                var r = await call("GET", "/UserItems/Resume", {userId: session.userId, mediaTypes: "Video", limit: 24, fields: itemFields, enableImageTypes: "Primary,Thumb,Backdrop", imageTypeLimit: 1});
                return (r && r.Items) || [];
            },

            nextUp: async function () {
                requireUser();
                var r = await call("GET", "/Shows/NextUp", {userId: session.userId, limit: 24, fields: itemFields, enableImageTypes: "Primary,Thumb,Backdrop", imageTypeLimit: 1});
                return (r && r.Items) || [];
            },

            seasons: async function (seriesId) { requireUser(); var r = await call("GET", "/Shows/" + encodeURIComponent(seriesId) + "/Seasons", {userId: session.userId}); return (r && r.Items) || []; },

            episodes: async function (seriesId, seasonId) {
                requireUser();
                var r = await call("GET", "/Shows/" + encodeURIComponent(seriesId) + "/Episodes", {userId: session.userId, seasonId: seasonId, fields: itemFields});
                return (r && r.Items) || [];
            },

            // The episode after `episode` in its series (crossing seasons), or null.
            nextEpisode: async function (episode) {
                requireUser();
                if (!episode || episode.Type !== "Episode" || !episode.SeriesId) return null;
                var r = await call("GET", "/Shows/" + encodeURIComponent(episode.SeriesId) + "/Episodes",
                    {userId: session.userId, startItemId: episode.Id, limit: 2, fields: itemFields, enableImageTypes: "Primary,Thumb,Backdrop", imageTypeLimit: 1});
                var items = (r && r.Items) || [];
                return items.length > 1 && items[1].Id !== episode.Id ? items[1] : null;
            },

            item: function (itemId) { requireUser(); return call("GET", "/Items/" + encodeURIComponent(itemId), {userId: session.userId}); },

            // PNG so the console's image path (proven with Moonlight artwork) can show it.
            imagePath: function (itemId, kind, tag, maxHeight) {
                return full("/Items/" + encodeURIComponent(itemId) + "/Images/" + (kind || "Primary") + query({format: "Png", maxHeight: maxHeight || 360, quality: 90, tag: tag}));
            },

            fetchImage: async function (itemId, kind, tag, maxHeight, signal) {
                var res = await http.fetchBytes(transport, {host: server.host, port: server.port, tls: server.tls, method: "GET", path: api.imagePath(itemId, kind, tag, maxHeight),
                    headers: {Authorization: authorization(), Accept: "image/png"}, timeoutMs: 15000, maxBytes: 3 * 1048576, signal: signal});
                if (res.status !== 200 || !res.body.length) return null;
                if (res.body[0] !== 0x89 || res.body[1] !== 0x50) return null; // not a PNG
                return http.toDataUrl(res.body, "image/png");
            },

            // HLS transcode to H.264 + stereo Opus in MPEG-TS: the only formats the
            // console's native pipeline (movie::Decoder + libopus) is known to accept.
            masterPlaylistPath: function (item, settings) {
                var source = (item.MediaSources && item.MediaSources[0]) || null;
                if (!source) throw new Error("This item has no playable media.");
                var playSessionId = randomHex(32);
                var p = {
                    DeviceId: device.id, MediaSourceId: source.Id, PlaySessionId: playSessionId,
                    VideoCodec: "h264", AudioCodec: "opus", SegmentContainer: "ts", MinSegments: 1,
                    BreakOnNonKeyFrames: true, TranscodingMaxAudioChannels: 2, MaxAudioChannels: 2,
                    AudioStreamIndex: settings.audioStreamIndex != null ? settings.audioStreamIndex : source.DefaultAudioStreamIndex,
                    // Subtitles are burned into the video by the server (Encode).
                    SubtitleStreamIndex: settings.subtitleStreamIndex != null && settings.subtitleStreamIndex >= 0 ? settings.subtitleStreamIndex : undefined,
                    SubtitleMethod: settings.subtitleStreamIndex != null && settings.subtitleStreamIndex >= 0 ? "Encode" : undefined,
                    VideoBitrate: settings.videoBitrate, AudioBitrate: 128000, MaxWidth: settings.maxWidth, MaxHeight: settings.maxHeight,
                    // Baseline forbids B-frames. The console's media path shows only the newest
                    // finished frame, and B-frames make the decoder release frames in pairs,
                    // which halved the frame rate (12 of 24 fps shown) on real hardware.
                    MaxFramerate: settings.maxFramerate, profile: "baseline", level: settings.level || "41",
                    AllowVideoStreamCopy: false, AllowAudioStreamCopy: false, EnableAutoStreamCopy: false, RequireAvc: false
                };
                return {path: full("/Videos/" + encodeURIComponent(item.Id) + "/master.m3u8" + query(p)), playSessionId: playSessionId, mediaSourceId: source.Id};
            },

            fetchText: async function (path, signal) {
                var res = await http.fetchBytes(transport, {host: server.host, port: server.port, tls: server.tls, method: "GET", path: path, headers: {Authorization: authorization()}, timeoutMs: 60000, maxBytes: 4 * 1048576, signal: signal});
                if (res.status >= 400) throw failure({status: res.status, data: null}, path);
                return http.decodeText(res.body);
            },

            // ---- Live TV ------------------------------------------------------
            liveTvInfo: function () { return call("GET", "/LiveTv/Info", {}); },
            channels: async function (startIndex, limit) {
                requireUser();
                var r = await call("GET", "/LiveTv/Channels", {userId: session.userId, startIndex: startIndex || 0, limit: limit || 100,
                    enableImages: true, imageTypeLimit: 1, enableImageTypes: "Primary", addCurrentProgram: true, enableUserData: false});
                return {items: (r && r.Items) || [], total: (r && r.TotalRecordCount) || 0};
            },
            // Programs on these channels overlapping [from, to) (Date objects).
            programs: async function (channelIds, from, to) {
                requireUser();
                if (!channelIds.length) return [];
                var r = await call("GET", "/LiveTv/Programs", {userId: session.userId, channelIds: channelIds.join(","),
                    minEndDate: from.toISOString(), maxStartDate: to.toISOString(), sortBy: "StartDate", sortOrder: "Ascending",
                    enableImages: false, enableUserData: false, enableTotalRecordCount: false, fields: "Overview", limit: 5000}, undefined, {maxBytes: 8 * 1048576});
                return (r && r.Items) || [];
            },
            // Opens the channel's tuner stream; returns {mediaSourceId, liveStreamId, playSessionId}.
            openLiveStream: async function (channelId, settings) {
                requireUser();
                var r = await call("POST", "/Items/" + encodeURIComponent(channelId) + "/PlaybackInfo", {}, {
                    UserId: session.userId, AutoOpenLiveStream: true, EnableDirectPlay: false, EnableDirectStream: false, EnableTranscoding: true,
                    AllowVideoStreamCopy: false, AllowAudioStreamCopy: false, MaxStreamingBitrate: settings.videoBitrate + 128000, MaxAudioChannels: 2
                }, {timeoutMs: 45000});
                var source = r && r.MediaSources && r.MediaSources[0];
                if (!source) throw new Error("The server did not open this channel" + (r && r.ErrorCode ? " (" + r.ErrorCode + ")." : "."));
                return {mediaSourceId: source.Id, liveStreamId: source.LiveStreamId || null, playSessionId: r.PlaySessionId || randomHex(32)};
            },
            // Live HLS (an EVENT playlist that grows), same formats as on-demand playback.
            livePlaylistPath: function (channelId, stream, settings) {
                var p = {
                    DeviceId: device.id, MediaSourceId: stream.mediaSourceId, LiveStreamId: stream.liveStreamId, PlaySessionId: stream.playSessionId,
                    VideoCodec: "h264", AudioCodec: "opus", SegmentContainer: "ts", MinSegments: 2, profile: "baseline", level: settings.level || "41",
                    VideoBitrate: settings.videoBitrate, AudioBitrate: 128000, MaxAudioChannels: 2, TranscodingMaxAudioChannels: 2,
                    Width: settings.maxWidth, MaxFramerate: 30, DeInterlace: true,
                    AllowVideoStreamCopy: false, AllowAudioStreamCopy: false, EnableAutoStreamCopy: false, RequireAvc: false
                };
                return full("/Videos/" + encodeURIComponent(channelId) + "/live.m3u8" + query(p));
            },
            closeLiveStream: function (liveStreamId) { return call("POST", "/LiveStreams/Close", {liveStreamId: liveStreamId}); },

            reportStart: function (state) { return call("POST", "/Sessions/Playing", null, report(state)); },
            reportProgress: function (state) { var r = report(state); r.EventName = "TimeUpdate"; return call("POST", "/Sessions/Playing/Progress", null, r); },
            reportStopped: function (state) { return call("POST", "/Sessions/Playing/Stopped", null, report(state)); },
            // This device's session, including TranscodingInfo while transcoding.
            sessionInfo: async function () {
                var sessions = await call("GET", "/Sessions", {deviceId: device.id});
                return (sessions || [])[0] || null;
            },
            stopTranscode: function (playSessionId) { return call("DELETE", "/Videos/ActiveEncodings", {deviceId: device.id, playSessionId: playSessionId}); }
        };

        function report(state) {
            return {ItemId: state.itemId, MediaSourceId: state.mediaSourceId, PlaySessionId: state.playSessionId,
                PositionTicks: Math.max(0, Math.round((state.position || 0) * TICKS)), IsPaused: !!state.paused,
                CanSeek: !state.liveStreamId, PlayMethod: "Transcode", LiveStreamId: state.liveStreamId || undefined};
        }

        api.headers = function () { return {Authorization: authorization()}; };
        return api;
    }

    // Parses what the user typed in the server field:
    //   [http:// | https://] (IPv4 | server name) [:port] [/base/path]
    // Default ports: https 443, http 8096 (Jellyfin's own). Returns
    // {scheme, tls, host, port, basePath, isIp, privateIp}.
    function parseServerAddress(input) {
        var text = String(input || "").trim();
        if (!text) throw new Error("Set the server address first.");
        var m = /^(?:([a-zA-Z][a-zA-Z0-9+.-]*):\/\/)?([^\/?#]*)([^?#]*)?(.*)$/.exec(text);
        var scheme = (m[1] || "http").toLowerCase(), authority = m[2] || "", path = m[3] || "";
        if (scheme !== "http" && scheme !== "https") throw new Error("Unsupported address type \"" + scheme + "://\". Use http:// or https://.");
        if (authority.indexOf("@") >= 0) throw new Error("Remove the user name from the address; sign in on the next screen.");
        if (authority[0] === "[") throw new Error("IPv6 addresses aren't supported yet. Use an IPv4 address or a server name.");
        var hp = /^([^:]*)(?::(\d{1,5}))?$/.exec(authority);
        if (!hp || !hp[1]) throw new Error("Enter an address such as http://192.168.1.20:8096 or https://jellyfin.example.com.");
        var host = hp[1].toLowerCase().replace(/\.$/, ""), port = hp[2] ? Number(hp[2]) : (scheme === "https" ? 443 : 8096);
        var isIp = /^\d{1,3}(\.\d{1,3}){3}$/.test(host);
        if (isIp && host.split(".").some(function (o) { return Number(o) > 255; })) throw new Error("\"" + host + "\" is not a valid IPv4 address.");
        if (!isIp && !(JF.dns ? JF.dns.validName(host) : /^[a-z0-9.-]+$/.test(host))) throw new Error("\"" + host + "\" is not a valid server name.");
        if (port < 1 || port > 65535) throw new Error("Port must be between 1 and 65535.");
        if (m[4]) throw new Error("Remove the ? or # part from the address.");
        var basePath = path.replace(/\/+$/, "");
        if (basePath && !/^(\/[A-Za-z0-9._~!$&'()*+,;=:@%-]+)+$/.test(basePath)) throw new Error("The path \"" + path + "\" contains characters that aren't allowed.");
        var o = isIp ? host.split(".").map(Number) : null;
        var privateIp = !!o && (o[0] === 10 || o[0] === 127 || (o[0] === 192 && o[1] === 168) || (o[0] === 172 && o[1] >= 16 && o[1] <= 31) || (o[0] === 169 && o[1] === 254) || (o[0] === 100 && o[1] >= 64 && o[1] <= 127));
        return {scheme: scheme, tls: scheme === "https", host: host, port: port, basePath: basePath, isIp: isIp, privateIp: privateIp};
    }

    // Plain HTTP that leaves the home network exposes the password and token.
    function insecureRemote(address) { return !address.tls && !address.privateIp; }

    // Picks a wide (16:9) image for an item: {id, kind, tag, fill}. fill=false
    // means only a tall poster exists and it should be centred, not stretched.
    function landscapeImage(it) {
        var tags = it.ImageTags || {};
        if (tags.Thumb) return {id: it.Id, kind: "Thumb", tag: tags.Thumb, fill: true};
        if (it.Type === "Episode" && tags.Primary) return {id: it.Id, kind: "Primary", tag: tags.Primary, fill: true};
        if (it.ParentThumbItemId && it.ParentThumbImageTag) return {id: it.ParentThumbItemId, kind: "Thumb", tag: it.ParentThumbImageTag, fill: true};
        if (it.BackdropImageTags && it.BackdropImageTags.length) return {id: it.Id, kind: "Backdrop", tag: it.BackdropImageTags[0], fill: true};
        if (it.ParentBackdropItemId && it.ParentBackdropImageTags && it.ParentBackdropImageTags.length)
            return {id: it.ParentBackdropItemId, kind: "Backdrop", tag: it.ParentBackdropImageTags[0], fill: true};
        if (tags.Primary) return {id: it.Id, kind: "Primary", tag: tags.Primary, fill: (it.PrimaryImageAspectRatio || 0) >= 1.2};
        if (it.SeriesPrimaryImageTag && it.SeriesId) return {id: it.SeriesId, kind: "Primary", tag: it.SeriesPrimaryImageTag, fill: false};
        return null;
    }

    JF.jellyfin = {landscapeImage: landscapeImage, create: create, query: query, TICKS: TICKS, parseServerAddress: parseServerAddress, insecureRemote: insecureRemote};
    if (typeof module !== "undefined" && module.exports) module.exports = JF.jellyfin;
})();

;
/* Playback engine: HLS (MPEG-TS) download -> demux -> timed delivery to a media sink.
 * Platform-independent; the sink, clock, scheduler and API are injected.
 *
 * Sink interface:
 *   setup({width, height})            once per session, before the first frame
 *   videoFrame(au) -> boolean         false = decoder busy, retry later
 *   audioFrame(packet)
 *   drainVideo() / drainAudio()       called every tick
 *   close()                           release native resources (sink may be reused)
 *
 * The native audio path keeps only ~60 ms of PCM and drops anything fed faster
 * than real time, so audio is released by timestamp, never in bulk. */
/* Credits: the sink interface (videoSetup/videoFrame/audioFrame/drain) is shaped
 * around XITRIX's MoonlightStreamMedia from Moonlight-CivSwitch, which does the
 * native decoding and output on the console. */
(function () {
    "use strict";
    var G = typeof window !== "undefined" ? window : globalThis;
    var JF = G.JF = G.JF || {};

    var START_BUFFER = 2.0;     // seconds buffered before (re)starting the clock
    var MAX_AHEAD = 24;         // stop downloading this far ahead of the playhead
    var EARLY_AHEAD = 6;        // ...but start with a short read-ahead and grow it, so the
                                // first seconds are not one long download burst (that is
                                // when the console's audio briefly ran dry in field reports)
    // The native presenter shows whichever frame finished decoding last, so each
    // frame must reach the decoder exactly as early as the decoder will delay it.
    // That delay (reordering + processing) is measured at runtime via sink.latency().
    var VIDEO_PER_TICK = 2;     // never hand over a burst that would skip frames
    var LIVE_EDGE_SEGMENTS = 3; // live streams start this many segments behind the newest
    var AUDIO_LEAD = 0.05;      // hand audio over this far before its time
    var LATE_AUDIO = 0.25;      // drop audio older than this
    var LATE_VIDEO = 1.5;       // beyond this, skip ahead to the next keyframe

    function create(deps) {
        var api = deps.api, sink = deps.sink, http = JF.http, hls = JF.hls, sps = JF.h264Sps, demuxer = JF.tsDemux;
        var now = deps.now || function () { return Date.now(); };
        var log = deps.log || function () {};
        var onState = deps.onState || function () {};

        var playlist = null, playlistPath = null, state = "idle", generation = 0, live = false;
        var videoQ = [], audioQ = [], ptsBase = null, sessionStart = 0, reorder = 0;
        var clockStart = null, clockPos = 0, paused = false, buffering = true, ended = false;
        var downloading = false, downloadDone = false, abort = {aborted: false};
        var configured = false, lastBuffered = 0, error = null, warnedAudio = false, duration = 0;
        var stats = {bframes: false, segments: 0, bytes: 0, videoFed: 0, audioFed: 0, audioDropped: 0, videoSkipped: 0, stalls: 0, maxTickGapMs: 0, lateFrames: 0};
        var videoInfo = null, lastTickAt = null, offsetSum = 0, offsetCount = 0, latency = null, latencyAt = -1e9;

        function setState(next, detail) {
            if (state === next && !detail) return;
            state = next;
            try { onState(next, detail || null); } catch (e) { log("state handler: " + e.message); }
        }

        function seconds(ts) { return (ts - ptsBase) / 90000 + sessionStart; }

        function position() {
            if (clockStart === null || paused || buffering) return clockPos;
            return clockPos + (now() - clockStart) / 1000;
        }

        function bufferedUntil() {
            var v = videoQ.length ? seconds(videoQ[videoQ.length - 1].pts) : null;
            var a = audioQ.length ? seconds(audioQ[audioQ.length - 1].pts) : null;
            if (v === null) return a === null ? position() : a;
            if (a === null) return v;
            return Math.min(v, a);
        }

        function freezeClock() { clockPos = position(); clockStart = null; }

        // ---- Download -------------------------------------------------------

        async function loadPlaylist(path, signal) {
            var text = await api.fetchText(path, signal);
            var variant = hls.firstVariant(text);
            if (variant) {
                playlistPath = hls.resolve(path, variant);
                text = await api.fetchText(playlistPath, signal);
            } else playlistPath = path;
            var parsed = hls.parsePlaylist(text);
            if (parsed.fmp4) throw new Error("Server sent fMP4 segments; MPEG-TS is required.");
            if (parsed.encrypted) throw new Error("Encrypted HLS is not supported.");
            if (!parsed.segments.length) throw new Error("The stream has no segments.");
            return parsed;
        }

        async function download(fromIndex, myGeneration, signal) {
            downloading = true; downloadDone = false;
            try {
                for (var i = fromIndex; ; i++) {
                    if (i >= playlist.segments.length) {
                        if (!live || playlist.endList) break;
                        // Live (EVENT or sliding) playlist: wait for the next segment, re-read it,
                        // and continue from the same sequence number.
                        var sequence = playlist.mediaSequence + i;
                        await deps.sleep(Math.max(1000, (playlist.targetDuration || 3) * 500));
                        if (signal.aborted || myGeneration !== generation) return;
                        var fresh = hls.parsePlaylist(await api.fetchText(playlistPath, signal));
                        if (fresh.fmp4 || fresh.encrypted) throw new Error("The live stream changed to an unsupported format.");
                        if (sequence < fresh.mediaSequence) log("Live playlist moved past segment " + sequence + "; continuing from " + fresh.mediaSequence + ".");
                        playlist = fresh; duration = fresh.totalDuration;
                        i = Math.max(0, sequence - fresh.mediaSequence) - 1;   // the loop's i++ lands on it
                        continue;
                    }
                    // Back-pressure: wait while far enough ahead of the playhead.
                    while (!signal.aborted && bufferedUntil() - position() > aheadLimit()) await deps.sleep(250);
                    if (signal.aborted || myGeneration !== generation) return;
                    var segment = playlist.segments[i];
                    var path = hls.resolve(playlistPath, segment.uri);
                    var demux = demuxer.create({
                        onVideo: function (au) { if (myGeneration === generation) acceptVideo(au); },
                        onAudio: function (pkt) { if (myGeneration === generation) acceptAudio(pkt); },
                        onUnsupportedAudio: function (info) {
                            if (!warnedAudio) { warnedAudio = true; log("Audio stream type 0x" + info.streamType.toString(16) + " is not Opus; playing without sound."); }
                        }
                    });
                    var attempt = 0;
                    for (;;) {
                        try {
                            await deps.transport.request({
                                host: api.server.host, port: api.server.port, tls: api.server.tls, method: "GET", path: path,
                                headers: api.headers(), timeoutMs: 90000, signal: signal
                            }, {
                                onHead: function (head) { if (head.status !== 200) throw new Error("Segment request failed: HTTP " + head.status); },
                                onBody: function (chunk) { stats.bytes += chunk.length; demux.push(chunk); }
                            });
                            demux.end();
                            break;
                        } catch (e) {
                            if (signal.aborted || myGeneration !== generation) return;
                            // The first segment can take a while while the server starts transcoding.
                            if (++attempt >= 3) throw e;
                            log("Segment " + i + " retry " + attempt + ": " + e.message);
                            await deps.sleep(1000 * attempt);
                        }
                    }
                    stats.segments++;
                }
                if (myGeneration === generation) downloadDone = true;
            } finally {
                if (myGeneration === generation) downloading = false;
            }
        }

        // Read-ahead grows by half a second per second played: 6 s -> 24 s in ~36 s.
        function aheadLimit() {
            return Math.min(MAX_AHEAD, EARLY_AHEAD + Math.max(0, position() - sessionStart) * 0.5);
        }

        function acceptVideo(au) {
            if (ptsBase === null) ptsBase = Math.min(au.pts, au.dts);
            if (au.pts - au.dts > reorder) { reorder = au.pts - au.dts; stats.bframes = true; }
            offsetSum += (au.pts - au.dts) / 90000; offsetCount++;
            videoQ.push(au);
        }

        // How far ahead of a frame's decode timestamp to hand it over so that it is
        // shown on time: measured feed-to-display delay minus the stream's own
        // presentation offset. Before measurements exist, assume one frame of work.
        function videoLead() {
            var t = now();
            if (sink.latency && t - latencyAt > 500) {
                latencyAt = t;
                var measured = sink.latency();
                if (measured !== null && measured !== undefined && isFinite(measured)) latency = measured;
            }
            var offset = offsetCount ? offsetSum / offsetCount : 0;
            var expected = latency !== null ? latency : offset + 0.04;
            stats.videoLeadMs = Math.round((expected - offset) * 1000);
            return Math.max(-0.1, Math.min(0.6, expected - offset));
        }

        function acceptAudio(pkt) {
            if (ptsBase === null) ptsBase = pkt.pts;
            audioQ.push(pkt);
        }

        // ---- Delivery (called by the host's frame loop) ---------------------

        function ensureConfigured() {
            if (configured) return true;
            // Drop anything before the first decodable keyframe.
            while (videoQ.length && !(videoQ[0].keyframe && videoQ[0].sps && videoQ[0].pps)) { videoQ.shift(); stats.videoSkipped++; }
            if (!videoQ.length) return false;
            var info = sps.parse(videoQ[0].sps);
            if (info.width > 1920 || info.height > 1080) throw new Error("Stream is " + info.width + "x" + info.height + "; the console decoder supports up to 1920x1080.");
            sink.setup({width: info.width, height: info.height});
            configured = true; videoInfo = info;
            log("Video " + info.width + "x" + info.height + " (H.264 profile " + info.profile + ", level " + info.level + ")");
            return true;
        }

        function skipToKeyframe(pos) {
            for (var i = 1; i < videoQ.length; i++) {
                if (videoQ[i].keyframe && seconds(videoQ[i].pts) >= pos) {
                    stats.videoSkipped += i; videoQ.splice(0, i);
                    log("Video fell behind; skipped " + i + " frames to the next keyframe.");
                    return;
                }
            }
        }

        function tick() {
            if (state === "idle" || state === "error" || state === "stopped") return;
            var tickAt = now();
            if (lastTickAt !== null && state === "playing" && tickAt - lastTickAt > stats.maxTickGapMs) stats.maxTickGapMs = tickAt - lastTickAt;
            lastTickAt = tickAt;
            try {
                var ahead = bufferedUntil() - position();
                if (buffering) {
                    var ready = ahead >= START_BUFFER || (downloadDone && (videoQ.length || audioQ.length));
                    if (ready && !paused && ensureConfigured()) {
                        buffering = false; clockStart = now();
                        if (videoQ.length && clockPos < seconds(videoQ[0].pts) - 0.5) clockPos = Math.max(clockPos, seconds(videoQ[0].pts));
                        setState("playing");
                    } else if (downloadDone && !videoQ.length && !audioQ.length) { finish(); return; }
                    else { lastBuffered = ahead; return; }
                }
                if (paused) { sink.drainVideo(); return; }

                var pos = position();
                // Video: keep decode order; release once its presentation time is close.
                if (videoQ.length && seconds(videoQ[0].dts) + reorder / 90000 < pos - LATE_VIDEO) skipToKeyframe(pos);
                var fed = 0, lead = videoLead();
                while (videoQ.length && fed < VIDEO_PER_TICK && seconds(videoQ[0].dts) <= pos + lead) {
                    if (seconds(videoQ[0].dts) + reorder / 90000 < pos - 0.1) stats.lateFrames++;
                    if (!sink.videoFrame(videoQ[0])) break;
                    videoQ.shift(); stats.videoFed++; fed++;
                }
                sink.drainVideo();

                // Audio: drop late packets, release due ones.
                while (audioQ.length && seconds(audioQ[0].pts) < pos - LATE_AUDIO) { audioQ.shift(); stats.audioDropped++; }
                var released = 0;
                while (audioQ.length && released < 8 && seconds(audioQ[0].pts) <= pos + AUDIO_LEAD) {
                    sink.audioFrame(audioQ.shift()); stats.audioFed++; released++;
                }
                sink.drainAudio();

                if (!videoQ.length && !audioQ.length) {
                    if (downloadDone) { if (pos >= duration - 0.5 || !downloading) finish(); }
                    else { freezeClock(); buffering = true; stats.stalls++; setState("buffering"); }
                }
            } catch (e) { fail(e); }
        }

        // ---- Control --------------------------------------------------------

        function resetSession() {
            generation++; abort.aborted = true; abort = {aborted: false};
            videoQ = []; audioQ = []; ptsBase = null; reorder = 0; configured = false;
            offsetSum = 0; offsetCount = 0; latency = null; latencyAt = -1e9;
            downloadDone = false; downloading = false; buffering = true; clockStart = null;
            try { sink.close(); } catch (e) { log("sink close: " + e.message); }
        }

        function startAt(seconds_, forcedIndex) {
            resetSession();
            var index = forcedIndex !== undefined ? forcedIndex : hls.segmentAt(playlist, Math.max(0, seconds_));
            sessionStart = playlist.segments[index].start;
            clockPos = Math.max(seconds_, sessionStart);
            var myGeneration = generation, signal = abort;
            setState("buffering");
            download(index, myGeneration, signal).catch(function (e) { if (myGeneration === generation && !signal.aborted) fail(e); });
        }

        function fail(e) {
            error = e; log("Playback error: " + (e && e.message));
            resetSession(); setState("error", e);
        }

        function finish() {
            if (ended) return;
            ended = true; freezeClock(); resetSession(); setState("ended");
        }

        return {
            load: async function (path, startSeconds) {
                setState("loading");
                ended = false; error = null;
                try {
                    var signal = abort;
                    playlist = await loadPlaylist(path, signal);
                    duration = playlist.totalDuration;
                    live = !playlist.endList;
                    if (live) {
                        var edge = Math.max(0, playlist.segments.length - LIVE_EDGE_SEGMENTS);
                        log("Live stream: starting at segment " + (playlist.mediaSequence + edge) + " of " + (playlist.mediaSequence + playlist.segments.length));
                        startAt(playlist.segments[edge].start, edge);
                    } else startAt(startSeconds || 0);
                } catch (e) { fail(e); }
            },
            tick: tick,
            pause: function () { if (paused || state === "ended") return; freezeClock(); paused = true; setState("paused"); },
            resume: function () {
                if (!paused) return; paused = false;
                if (!buffering) { clockStart = now(); setState("playing"); } else setState("buffering");
            },
            toggle: function () { if (paused) this.resume(); else this.pause(); },
            seek: function (target) {
                if (!playlist || live) return;                          // no seeking in live TV
                target = Math.max(0, Math.min(duration - 1, target));
                ended = false; startAt(target);
            },
            stop: function () { resetSession(); setState("stopped"); },
            position: position,
            duration: function () { return duration; },
            buffered: function () { return Math.max(0, bufferedUntil() - position()); },
            state: function () { return state; },
            paused: function () { return paused; },
            error: function () { return error; },
            stats: stats,
            videoInfo: function () { return videoInfo; },
            live: function () { return live; }
        };
    }

    JF.player = {create: create};
    if (typeof module !== "undefined" && module.exports) module.exports = JF.player;
})();

;
/* Console platform: Nintendo SDK calls through CivSwitch's loader helpers
 * (call_rop, malloc, read32, ...), which exist only inside the loader's scope.
 *
 * - resolveNativeSymbol: export lookup in the SDK module's ELF hash table.
 * - transport: HTTP/1.1 over nn::socket (IPv4, plain HTTP). Sockets read and
 *   write straight into a validated JS ArrayBuffer through MoonlightStreamIO,
 *   avoiding per-byte native memory access.
 * - storage: one JSON settings file under save:/civhomebrew/jellyfin. */
/* Credits: this file runs on the low-level primitives of Gezine's CivSwitch loader
 * (call_rop, malloc, read32, alloc_string, ...; https://github.com/Gezine/CivSwitch, MIT).
 * The SDK export lookup, the nn::socket call sequence and errno handling, and the
 * nn::fs save-file pattern follow XITRIX's Moonlight-CivSwitch
 * (app/src/platform/native-network.js). The controller button reading (nn::hid
 * styles, state layout, controller ids) follows XITRIX's CivHomebrew
 * (app/src/platform/native.js). Socket I/O goes through MoonlightStreamIO from
 * Moonlight-CivSwitch's native-io.js, bundled at build time. */
(function () {
    "use strict";
    var G = typeof window !== "undefined" ? window : globalThis;
    var JF = G.JF = G.JF || {};

    function available() {
        return typeof sdk_base !== "undefined" && typeof call_rop === "function" && typeof malloc === "function";
    }

    function createResolver() {
        var cache = {};
        var mod = sdk_base + read32(sdk_base + 4n);
        if (read32(mod) !== 0x30444f4dn) throw new Error("Unrecognized SDK module.");
        var dynamic = mod + BigInt.asIntN(32, read32(mod + 4n)), tags = {};
        for (var i = 0; i < 128; i++) {
            var tag = Number(read64(dynamic + BigInt(i * 16)));
            if (!tag) break;
            var value = read64(dynamic + BigInt(i * 16 + 8));
            tags[tag] = value < sdk_base ? sdk_base + value : value;
        }
        var table = tags[4], strings = tags[5], symbols = tags[6];
        if (!table || !strings || !symbols) throw new Error("SDK export table unavailable.");
        var buckets = Number(read32(table)), count = Number(read32(table + 4n));
        return function resolve(name) {
            if (cache[name]) return cache[name];
            var hash = 0;
            for (var j = 0; j < name.length; j++) {
                hash = ((hash << 4) + name.charCodeAt(j)) >>> 0;
                var high = hash & 0xf0000000;
                if (high) hash ^= high >>> 24;
                hash = (hash & ~high) >>> 0;
            }
            var index = Number(read32(table + 8n + BigInt((hash % buckets) * 4)));
            for (var k = 0; index && k < count; k++) {
                var symbol = symbols + BigInt(index * 24);
                if (read_null_terminated_string(strings + read32(symbol)) === name) {
                    var offset = read64(symbol + 8n);
                    if (!offset) break;
                    return (cache[name] = sdk_base + offset);
                }
                index = Number(read32(table + 8n + BigInt((buckets + index) * 4)));
            }
            throw new Error("Missing SDK export: " + name);
        };
    }

    function sleep(ms) { return new Promise(function (r) { setTimeout(r, ms); }); }

    // One shared, validated heap: [0, 1 MiB) network slots, the rest for media.
    var HEAP_BYTES = 8 * 1048576, NET_BYTES = 1048576, SLOT = 256 * 1024, SEND = 32 * 1024;

    // options.resolveNativeSymbol replaces the SDK export lookup (used by the simulated-console tests).
    function create(options) {
        options = options || {};
        if (!available()) throw new Error("This app must be started from CivSwitch / CivHomebrew.");
        if (!G.MoonlightStreamIO) throw new Error("Native I/O module missing from the package.");
        var resolveNativeSymbol = options.resolveNativeSymbol || createResolver();
        var io = G.MoonlightStreamIO.create({resolveNativeSymbol: resolveNativeSymbol}, {batchReceive: false});
        var heap = new Uint8Array(new ArrayBuffer(HEAP_BYTES));
        var heapBase = null;
        function base() { if (heapBase === null) heapBase = io.heapAddress(heap); return heapBase; }

        function sock(name, args) { return Number(BigInt.asIntN(32, io.invoke("_ZN2nn6socket" + name, args))); }
        function errno() { return Number(io.invoke("_ZN2nn6socket12GetLastErrnoEv", [])); }

        // Fixed slots so concurrent requests (artwork while streaming) never share buffers.
        var slots = [], waiting = [];
        for (var s = 0; s < NET_BYTES / SLOT; s++) slots.push({offset: s * SLOT, busy: false});
        function acquire() {
            for (var i = 0; i < slots.length; i++) if (!slots[i].busy) { slots[i].busy = true; return Promise.resolve(slots[i]); }
            return new Promise(function (resolve) { waiting.push(resolve); });
        }
        function release(slot) {
            var next = waiting.shift();
            if (next) next(slot); else slot.busy = false;
        }

        // ---- Name lookup ----------------------------------------------------
        // Public DNS over UDP first (UDP is proven on the console by Moonlight's
        // streaming); the console's own resolver is a fallback for names only the
        // local network knows. Results are cached for their TTL (60 s to 1 h).
        var dnsCache = {}, systemResolver;
        var DNS_SERVERS = [[1, 1, 1, 1], [8, 8, 8, 8], [9, 9, 9, 9]];
        function isIpv4(host) { return /^\d{1,3}(\.\d{1,3}){3}$/.test(host) && host.split(".").every(function (o) { return Number(o) <= 255; }); }

        async function udpLookup(host, server, slot, timeoutMs) {
            var id = Math.floor(Math.random() * 65536), query = JF.dns.buildQuery(id, host);
            var addrAt = slot.offset, queryAt = slot.offset + 64, recvAt = slot.offset + SEND;
            heap.set([16, 2, 0, 53].concat(server, [0, 0, 0, 0, 0, 0, 0, 0]), addrAt);
            heap.set(query, queryAt);
            var fd = sock("6SocketEiii", [2, 2, 0]);
            if (fd < 0) throw new Error("Cannot create a UDP socket.");
            try {
                var flags = sock("5FcntlEiiz", [fd, 3, 0]);
                if (flags < 0 || sock("5FcntlEiiz", [fd, 4, flags | 0x800]) < 0) throw new Error("Cannot configure the UDP socket.");
                if (sock("6SendToEiPKvmiPK8sockaddrj", [fd, base() + BigInt(queryAt), query.length, 0, base() + BigInt(addrAt), 16]) !== query.length) throw new Error("DNS query was not sent (errno " + errno() + ").");
                var deadline = Date.now() + timeoutMs;
                while (Date.now() < deadline) {
                    var got = sock("8RecvFromEiPvmiP8sockaddrPj", [fd, base() + BigInt(recvAt), 1500, 0x80, 0, 0]);
                    if (got > 0) {
                        try { return JF.dns.parseResponse(heap.slice(recvAt, recvAt + got), id, host); }
                        catch (e) { if (/different query|Not a DNS/.test(e.message)) continue; throw e; }
                    }
                    var e2 = errno();
                    if (got < 0 && e2 !== 11 && e2 !== 35) throw new Error("DNS receive failed (errno " + e2 + ").");
                    await sleep(10);
                }
                throw new Error("No DNS reply from " + server.join("."));
            } finally { sock("5CloseEi", [fd]); }
        }

        function systemLookup(host) {
            if (systemResolver === undefined) {
                systemResolver = null;
                ["_ZN2nn6socket16GetHostEntByNameEPKc", "_ZN2nn6socket13GetHostByNameEPKc"].some(function (name) {
                    try { systemResolver = {name: name, address: resolveNativeSymbol(name)}; return true; } catch (e) { return false; }
                });
            }
            if (!systemResolver) throw new Error("the console's resolver is not available");
            var p = alloc_string(host);
            try {
                var ent = call_rop(systemResolver.address, p);                        // struct hostent*
                if (!ent) throw new Error("the console's resolver found no address");
                var type = Number(read32(ent + 16n)), length = Number(read32(ent + 20n)), list = read64(ent + 24n);
                if (type !== 2 || length !== 4 || !list) throw new Error("the console's resolver returned no IPv4 address");
                var first = read64(list);
                if (!first) throw new Error("the console's resolver returned an empty list");
                var v = Number(read32(first));
                return {addresses: [[v & 255, v >>> 8 & 255, v >>> 16 & 255, v >>> 24 & 255].join(".")], ttl: 300, via: "console resolver"};
            } finally { free(p); }
        }

        async function lookup(host, report) {
            if (isIpv4(host)) return host.split(".").map(Number);
            var cached = dnsCache[host];
            if (cached && cached.expires > Date.now()) return cached.octets;
            var slot = await acquire(), errors = [];
            try {
                for (var i = 0; i < DNS_SERVERS.length; i++) {
                    try {
                        var r = await udpLookup(host, DNS_SERVERS[i], slot, 1500);
                        return remember(host, r, "public DNS " + DNS_SERVERS[i].join("."), report);
                    } catch (e) {
                        errors.push(DNS_SERVERS[i].join(".") + ": " + e.message);
                        if (e.rcode === 3) break;                                   // the name does not exist publicly
                    }
                }
            } finally { release(slot); }
            try { return remember(host, systemLookup(host), "console resolver", report); }
            catch (e) { errors.push(e.message); }
            throw new Error("Cannot find \"" + host + "\": " + errors.join("; "));
        }
        function remember(host, result, via, report) {
            var octets = result.addresses[0].split(".").map(Number);
            dnsCache[host] = {octets: octets, expires: Date.now() + Math.max(60, Math.min(3600, result.ttl || 300)) * 1000};
            if (report) report("Found " + host + " at " + result.addresses[0] + " via " + via + (result.cname ? " (alias of " + result.cname + ")" : ""));
            return octets;
        }

        // ---- TLS (the console's nn::ssl) --------------------------------------
        // Function names are from the console's export table. Enum values follow the
        // Switch SDK: SslVersion Auto = 1; VerifyOption PeerCa|HostName|DateCheck = 7;
        // IoMode NonBlocking = 2. Untested on hardware; every step reports its result.
        var tlsVerify = true, WOULD_BLOCK = 123 | (204 << 9);
        function ssl(name, args) { return Number(BigInt.asUintN(32, io.invoke("_ZN2nn3ssl" + name, args))); }
        function resultText(r) { return r ? (2000 + (r & 0x1ff)) + "-" + String((r >>> 9) & 0x1fff).padStart(4, "0") : "ok"; }
        function zeroed(n) { var p = malloc(n); if (!p) throw new Error("Out of memory."); write_buffer(p, new Uint8Array(n)); return p; }

        // owner.ownsSocket is set as soon as the socket is handed to the TLS service, so the
        // caller never closes it again, even if the handshake then fails.
        async function tlsOpen(fd, host, check, report, owner) {
            var t = {ctx: zeroed(256), conn: zeroed(256), out: zeroed(16), ctxMade: false, connMade: false, ownsSocket: false};
            function step(label, r) { if (r) throw new Error("TLS " + label + " failed: " + resultText(r)); }
            try {
                ssl("7ContextC1Ev", [t.ctx]);
                step("context", ssl("7Context6CreateENS1_10SslVersionE", [t.ctx, 1])); t.ctxMade = true;
                ssl("10ConnectionC1Ev", [t.conn]);
                step("connection", ssl("10Connection6CreateEPNS0_7ContextE", [t.conn, t.ctx])); t.connMade = true;
                step("socket handoff", ssl("10Connection19SetSocketDescriptorEi", [t.conn, fd])); t.ownsSocket = owner.ownsSocket = true;
                var name = alloc_string(host);
                try { step("server name", ssl("10Connection11SetHostNameEPKcj", [t.conn, name, host.length])); } finally { free(name); }
                step("verify option", ssl("10Connection15SetVerifyOptionENS1_12VerifyOptionE", [t.conn, tlsVerify ? 7 : 0]));
                step("non-blocking mode", ssl("10Connection9SetIoModeENS1_6IoModeE", [t.conn, 2]));
                for (;;) {
                    check();
                    var r = ssl("10Connection11DoHandshakeEv", [t.conn]);
                    if (!r) break;
                    if (r === WOULD_BLOCK) { await sleep(10); continue; }
                    var detail = "";
                    if (!ssl("10Connection18GetVerifyCertErrorEPNS_6ResultE", [t.conn, t.out])) {
                        var v = Number(read32(t.out));
                        if (v) detail = " (certificate problem " + resultText(v) + (tlsVerify ? "; if the server uses a self-signed certificate, turn off certificate checking in Settings" : "") + ")";
                    }
                    throw new Error("TLS handshake failed: " + resultText(r) + detail);
                }
                if (report) report("Secure connection established" + (tlsVerify ? " (certificate verified)" : " (certificate NOT checked)"));
                return t;
            } catch (e) { tlsClose(t); throw e; }
        }
        function tlsWrite(t, address, length) {
            var r = ssl("10Connection5WriteEPKcPij", [t.conn, address, t.out, length]);
            if (r === WOULD_BLOCK) return 0;
            if (r) throw new Error("TLS write failed: " + resultText(r));
            return Number(read32(t.out));
        }
        // > 0 bytes read, 0 would block, -1 closed or failed (detail in t.lastError).
        function tlsRead(t, address, length) {
            var r = ssl("10Connection4ReadEPcPij", [t.conn, address, t.out, length]);
            if (r === WOULD_BLOCK) return 0;
            if (r) { t.lastError = resultText(r); return -1; }
            var n = Number(read32(t.out));
            return n > 0 ? n : -1;
        }
        function tlsClose(t) {
            if (!t) return;
            try { if (t.connMade) ssl("10Connection7DestroyEv", [t.conn]); } catch (e) {}
            try { ssl("10ConnectionD1Ev", [t.conn]); } catch (e) {}
            try { if (t.ctxMade) ssl("7Context7DestroyEv", [t.ctx]); } catch (e) {}
            try { ssl("7ContextD1Ev", [t.ctx]); } catch (e) {}
            free(t.out); free(t.conn); free(t.ctx);
        }

        // ---- HTTP over TCP (plain or TLS) -------------------------------------
        async function request(options, handlers) {
            var port = options.port || (options.tls ? 443 : 80), report = options.report;
            if (!(port >= 1 && port <= 65535)) throw new Error("Invalid port.");
            var wire = JF.http.buildRequest(options);
            if (wire.length > SEND - 64) throw new Error("Request too large.");
            var timeout = options.timeoutMs || 15000, deadline = Date.now() + timeout;
            var signal = options.signal || {aborted: false};
            function check() {
                if (signal.aborted) throw new Error("Request canceled.");
                if (Date.now() > deadline) throw new Error("The server did not respond in time.");
            }
            var octets = await lookup(options.host, report);
            var slot = await acquire(), fd = -1, tls = null, tlsOwner = {ownsSocket: false}, head = null, ended = false;
            var recvOffset = slot.offset + SEND, recvBytes = 32768;
            try {
                check();
                // sockaddr_in as the SDK expects it: length, family, port (BE), address.
                heap.set([16, 2, port >> 8, port & 255].concat(octets, [0, 0, 0, 0, 0, 0, 0, 0]), slot.offset);
                fd = sock("6SocketEiii", [2, 1, 0]);
                if (fd < 0) throw new Error("Cannot create a network socket.");
                var flags = sock("5FcntlEiiz", [fd, 3, 0]);
                if (flags < 0 || sock("5FcntlEiiz", [fd, 4, flags | 0x800]) < 0) throw new Error("Cannot configure the socket.");
                var connected = sock("7ConnectEiPK8sockaddrj", [fd, base() + BigInt(slot.offset), 16]);
                while (connected < 0) {
                    var e = errno();
                    if (e === 56 || e === 106) break;                       // already connected
                    if ([115, 114, 36, 37, 11, 35].indexOf(e) < 0) throw new Error(e === 111 || e === 61 ? "Connection refused. Is Jellyfin reachable on that address and port?" : "Cannot connect (errno " + e + ").");
                    check(); await sleep(15);
                    connected = sock("7ConnectEiPK8sockaddrj", [fd, base() + BigInt(slot.offset), 16]);
                }
                if (report) report("Connected to " + octets.join(".") + ":" + port);
                if (options.tls) { tls = await tlsOpen(fd, options.host, check, report, tlsOwner); }
                var wireAt = slot.offset + 64;
                heap.set(wire, wireAt);
                var sent = 0;
                while (sent < wire.length) {
                    check();
                    var n;
                    if (tls) n = tlsWrite(tls, base() + BigInt(wireAt + sent), wire.length - sent);
                    else {
                        n = sock("4SendEiPKvmi", [fd, base() + BigInt(wireAt + sent), wire.length - sent, 0x80]);
                        if (n < 0) { var se = errno(); if (se !== 11 && se !== 35) throw new Error("Network send failed (errno " + se + ")."); n = 0; }
                    }
                    if (n > 0) sent += n; else await sleep(5);
                }
                var parser = JF.http.createParser({
                    noBody: options.method === "HEAD",
                    onHead: function (h) { head = h; if (handlers.onHead) handlers.onHead(h); },
                    onBody: function (b) { if (handlers.onBody) handlers.onBody(b); },
                    onEnd: function () { ended = true; }
                });
                var lastYield = Date.now();
                while (!ended) {
                    check();
                    var got;
                    if (tls) got = tlsRead(tls, base() + BigInt(recvOffset), recvBytes);
                    else {
                        got = sock("4RecvEiPvmi", [fd, base() + BigInt(recvOffset), recvBytes, 0x80]);
                        if (got === 0) got = -1;
                        else if (got < 0) { var re = errno(); if (re !== 11 && re !== 35) throw new Error("Network receive failed (errno " + re + ")."); got = 0; }
                    }
                    if (got > 0) {
                        deadline = Date.now() + timeout;                     // inactivity timeout
                        var sliceStart = Date.now();
                        parser.push(heap.slice(recvOffset, recvOffset + got));
                        var slice = Date.now() - sliceStart;
                        if (slice > netStats.longestSliceMs) netStats.longestSliceMs = slice;
                        if (slice > netStats.windowMaxMs) netStats.windowMaxMs = slice;
                        // Keep the frame loop (video/audio delivery) running during big downloads.
                        if (Date.now() - lastYield >= 4) { await sleep(0); lastYield = Date.now(); }
                    } else if (got < 0) {
                        if (tls && !head) throw new Error("Secure connection closed before a response: " + (tls.lastError || "closed"));
                        parser.eof(); break;
                    } else { await sleep(4); lastYield = Date.now(); }
                }
                if (!head) throw new Error("No response from the server.");
                return {status: head.status, headers: head.headers};
            } finally {
                if (tls) tlsClose(tls);
                if (tlsOwner.ownsSocket) fd = -1;                           // the TLS service closed it
                if (fd >= 0) sock("5CloseEi", [fd]);
                release(slot);
            }
        }

        // Step-by-step check of an address, for the Test connection screen.
        async function testConnection(address, path) {
            var lines = [], ok = false, started = Date.now();
            function report(text) { lines.push(text); }
            try {
                report("Address: " + (address.tls ? "https" : "http") + "://" + address.host + ":" + address.port + (address.basePath || ""));
                var res = await JF.http.fetchJson({request: request}, {host: address.host, port: address.port, tls: address.tls, method: "GET",
                    path: (address.basePath || "") + path, headers: {Accept: "application/json"}, timeoutMs: 15000, maxBytes: 1048576, report: report});
                report("Jellyfin replied: HTTP " + res.status + (res.data && res.data.ServerName ? " from \"" + res.data.ServerName + "\" " + (res.data.Version || "") : ""));
                ok = res.status === 200 && !!(res.data && res.data.Id);
                if (!ok) report(res.status === 200 ? "That address answered, but not as a Jellyfin server. Check the path after the address." : "Unexpected reply; check the address and path.");
            } catch (e) { report("FAILED: " + e.message); }
            report("Took " + (Date.now() - started) + " ms");
            return {ok: ok, lines: lines};
        }

        var netStats = {longestSliceMs: 0, windowMaxMs: 0};

        // ---- Settings file -------------------------------------------------
        var ROOT = "save:/civhomebrew/jellyfin", FILE = ROOT + "/settings.json";
        function fs(name, args) { return call_rop.apply(null, [resolveNativeSymbol(name)].concat(args.map(BigInt))); }
        function code(v) { return Number(v & 0x3fffffn); }
        function pathCall(name, path, allowed) {
            var p = alloc_string(path);
            try { var r = code(fs(name, [p])); if (r && r !== allowed) throw new Error(name + " failed: 0x" + r.toString(16)); }
            finally { free(p); }
        }
        var storage = {
            load: function () {
                var p = alloc_string(FILE), handle = malloc(8), size = malloc(8), buffer = 0n, opened = false;
                try {
                    var r = code(fs("_ZN2nn2fs8OpenFileEPNS0_10FileHandleEPKci", [handle, p, 1]));
                    if (r === 0x202) return null;                            // not found
                    if (r) throw new Error("Settings open failed: 0x" + r.toString(16));
                    opened = true;
                    if (code(fs("_ZN2nn2fs11GetFileSizeEPlNS0_10FileHandleE", [size, read64(handle)]))) throw new Error("Settings size query failed.");
                    var length = Number(read64(size));
                    if (length <= 0 || length > 65536) return null;
                    buffer = malloc(length);
                    if (code(fs("_ZN2nn2fs8ReadFileENS0_10FileHandleElPvm", [read64(handle), 0, buffer, length]))) throw new Error("Settings read failed.");
                    return JSON.parse(bytes_to_string(read_buffer(buffer, length)));
                } catch (e) { JF.log && JF.log("Settings load: " + e.message); return null; }
                finally { if (opened) fs("_ZN2nn2fs9CloseFileENS0_10FileHandleE", [read64(handle)]); if (buffer) free(buffer); free(size); free(handle); free(p); }
            },
            save: function (data) {
                ensureDirs([]);
                writeFile(FILE, text_encode(JSON.stringify(data)));
                pathCall("_ZN2nn2fs6CommitEPKc", "save", 0);
            },
            // Text file under save:/civhomebrew/jellyfin/ (readable over CivHomebrew FTP).
            writeText: function (relative, text) {
                if (!/^[A-Za-z0-9_.-]+(\/[A-Za-z0-9_.-]+)*$/.test(relative) || relative.indexOf("..") >= 0) throw new Error("Invalid file name.");
                var parts = relative.split("/");
                ensureDirs(parts.slice(0, -1));
                writeFile(ROOT + "/" + relative, text_encode(text));
                pathCall("_ZN2nn2fs6CommitEPKc", "save", 0);
                return "civhomebrew/jellyfin/" + relative;
            }
        };

        function ensureDirs(sub) {
            pathCall("_ZN2nn2fs15CreateDirectoryEPKc", "save:/civhomebrew", 0x402);
            pathCall("_ZN2nn2fs15CreateDirectoryEPKc", ROOT, 0x402);
            for (var i = 0, path = ROOT; i < sub.length; i++) { path += "/" + sub[i]; pathCall("_ZN2nn2fs15CreateDirectoryEPKc", path, 0x402); }
        }

        function writeFile(path, bytes) {
            var p = alloc_string(path), handle = malloc(8), option = malloc(8), buffer = malloc(bytes.length + 1), opened = false;
            try {
                var created = code(fs("_ZN2nn2fs10CreateFileEPKcl", [p, bytes.length]));
                if (created && created !== 0x402) throw new Error("File create failed: 0x" + created.toString(16));
                if (code(fs("_ZN2nn2fs8OpenFileEPNS0_10FileHandleEPKci", [handle, p, 6]))) throw new Error("File open for write failed.");
                opened = true;
                if (code(fs("_ZN2nn2fs11SetFileSizeENS0_10FileHandleEl", [read64(handle), bytes.length]))) throw new Error("File resize failed.");
                write32(option, 1n); write_buffer(buffer, bytes);
                if (code(fs("_ZN2nn2fs9WriteFileENS0_10FileHandleElPKvmRKNS0_11WriteOptionE", [read64(handle), 0, buffer, bytes.length, option]))) throw new Error("File write failed.");
            } finally { if (opened) fs("_ZN2nn2fs9CloseFileENS0_10FileHandleE", [read64(handle)]); free(p); free(handle); free(option); free(buffer); }
        }

        // ---- Controller buttons --------------------------------------------
        // The engine only reports navigation, A and B as input events. Other
        // buttons are read from the controller directly through nn::hid, the
        // way CivHomebrew reads X: find the active controller, read its state,
        // and return the NpadButton bit set (A=0x1, B=0x2, X=0x4, Y=0x8,
        // L=0x40, R=0x80, ZL=0x100, ZR=0x200, Plus=0x400, Minus=0x800).
        function createController() {
            var prefix = "_ZN2nn3hid6detail", owned = [], selected = null, nextScan = 0, closed = false;
            var styleFn = resolveNativeSymbol(prefix + "15GetNpadStyleSetEPNS_4util10BitFlagSetILi32ENS0_12NpadStyleTagEEERKj");
            var getters = [[2, "17NpadHandheldState"], [1, "16NpadFullKeyState"], [4, "16NpadJoyDualState"]].map(function (g) {
                return {bit: g[0], address: resolveNativeSymbol(prefix + "13GetNpadStatesEPiPNS0_" + g[1] + "EiRKj")};
            });
            function take(n) { var p = malloc(n); if (!p) throw new Error("Controller allocation failed."); owned.push(p); return p; }
            var id = take(8), style = take(8), count = take(8), sample = take(64);
            function read(controller, getter) {
                write32(id, BigInt(controller)); write32(count, 0n);
                if (code(call_rop(getter.address, count, sample, 1n, id))) throw new Error("Controller state failed.");
                if (read32(count) !== 1n || !(read32(sample + 32n) & 1n)) return null;    // not connected
                return read64(sample + 8n);
            }
            return {
                poll: function () {
                    if (closed) return null;
                    if (selected) { var v = read(selected.id, selected.getter); if (v !== null) return v; selected = null; nextScan = 0; }
                    if (Date.now() < nextScan) return null;
                    nextScan = Date.now() + 500;
                    var ids = [32, 0, 1, 2, 3, 4, 5, 6, 7];                     // handheld first, then players 1-8
                    for (var i = 0; i < ids.length; i++) {
                        write32(id, BigInt(ids[i])); write32(style, 0n);
                        if (code(call_rop(styleFn, style, id))) throw new Error("Controller style failed.");
                        var bits = Number(read32(style));
                        for (var g = 0; g < getters.length; g++) {
                            if (!(bits & getters[g].bit)) continue;
                            var value = read(ids[i], getters[g]);
                            if (value !== null) { selected = {id: ids[i], getter: getters[g]}; return value; }
                        }
                    }
                    return null;
                },
                close: function () { if (closed) return; closed = true; owned.reverse().forEach(free); owned = []; }
            };
        }

        // ---- Keep the screen awake during playback -----------------------------
        // The Switch dims and sleeps without button presses. The SDK's media playback
        // section tells the system a video is playing; SetAutoSleepDisabled is the
        // fallback. Looking up a missing export is harmless; availability is logged.
        var awake = {fn: undefined, on: false};
        function keepAwake(on) {
            on = !!on;
            if (awake.fn === undefined) {
                awake.fn = null;
                try { awake.fn = {kind: "section", begin: resolveNativeSymbol("_ZN2nn2oe25BeginMediaPlaybackSectionEv"), end: resolveNativeSymbol("_ZN2nn2oe23EndMediaPlaybackSectionEv"), label: "media playback section"}; } catch (e) {}
                if (!awake.fn) try { awake.fn = {kind: "flag", set: resolveNativeSymbol("_ZN2nn2oe20SetAutoSleepDisabledEb"), label: "auto-sleep disabled"}; } catch (e) {}
                if (JF.log) JF.log(awake.fn ? "Screen stays awake during playback (" + awake.fn.label + ")" : "Keeping the screen awake is not available in this game's SDK");
            }
            if (!awake.fn || awake.on === on) return !!awake.fn;
            try {
                if (awake.fn.kind === "section") call_rop(on ? awake.fn.begin : awake.fn.end);
                else call_rop(awake.fn.set, on ? 1n : 0n);
                awake.on = on;
            } catch (e) { if (JF.log) JF.log("Keep-awake failed: " + e.message); awake.fn = null; }
            return true;
        }

        return {
            keepAwake: keepAwake,
            createController: createController,
            transport: {request: request, stats: netStats},
            testConnection: testConnection,
            setTlsVerify: function (on) { tlsVerify = on !== false; },
            storage: storage,
            createSink: function (options) { return JF.createNativeSink(io, heap, NET_BYTES, options); },
            sleep: sleep,
            close: function () { try { keepAwake(false); } catch (e) {} try { io.close(); } catch (e) {} }
        };
    }

    JF.nativePlatform = {available: available, create: create};
})();

;
/* Player sink backed by the console's native media pipeline (MoonlightStreamMedia,
 * bundled at build time from a local Moonlight-CivSwitch checkout):
 * movie::Decoder hardware H.264 -> GPU texture shown as a live:/ image, and
 * libopus -> nn::audio AudioOut at 48 kHz stereo. */
/* Credits: playback uses XITRIX's native media pipeline from Moonlight-CivSwitch:
 * native-io.js, native-texture.js and native-media.js (MoonlightStreamIO,
 * MoonlightNativeTexture, MoonlightStreamMedia), copied from your local checkout at
 * build time and not covered by this project's license. The synchronous
 * presentation fallback uses that module's own synchronousGpu option; the decoder
 * delay estimate reads its presentAge statistics. */
(function () {
    "use strict";
    var G = typeof window !== "undefined" ? window : globalThis;
    var JF = G.JF = G.JF || {};

    // heap: the platform's validated ArrayBuffer view; media uses [mediaOffset, end).
    function create(io, heap, mediaOffset, options) {
        options = options || {};
        if (!G.MoonlightStreamMedia || !G.MoonlightNativeTexture) throw new Error("Native media modules missing from the package.");
        var SPS = mediaOffset + 16, PPS = mediaOffset + 65536, AUDIO = mediaOffset + 131072, AU = mediaOffset + 262144;
        var AU_LIMIT = heap.length - AU;
        // Only the GPU path is used, which never touches the wasm core; these
        // members cover the few calls MoonlightStreamMedia makes regardless.
        var core = {HEAPU8: heap, _malloc: function () { return 0; }, _free: function () {}, _ml_png_release: function () {}, UTF8ToString: function () { return ""; }};
        var media = null, spsLength = 0, ppsLength = 0, failures = 0;
        var log = options.log || function () {};

        function open(format, synchronousGpu) {
            media = G.MoonlightStreamMedia.create(io, core, {
                onImage: options.onImage, onStatus: function (text) { log(text); },
                smoothVideo: false, synchronousGpu: synchronousGpu
            });
            // fps only selects Moonlight's 60 fps live pacer; timing is handled by the player.
            if (media.videoSetup(1, format.width, format.height, 30) !== 0) throw new Error("Video decoder setup failed.");
        }

        function setup(format) {
            close();
            // The asynchronous path needs a GPU fence found at fixed offsets in the
            // movie module. If those checks reject it, use the module's synchronous
            // presentation instead (no fence) and keep that choice for this session.
            if (!G.__jellyfinSyncGpu) {
                try { open(format, false); }
                catch (e) {
                    if (!/GPU sync|GPU completion/.test(e.message)) throw e;
                    log("Async GPU presentation unavailable (" + e.message + "); using synchronous presentation.");
                    try { if (media) media.close(); } catch (closeError) { log("Media cleanup: " + closeError.message); }
                    media = null; G.__jellyfinSyncGpu = true;
                }
            }
            if (!media) { open(format, true); log("Video: synchronous GPU presentation"); }
            else log("Video: asynchronous GPU presentation");
            if (media.audioSetup(48000, 2, 1, 1, 960) !== 0) throw new Error("Audio output setup failed.");
            spsLength = ppsLength = 0;
        }

        function videoFrame(au) {
            if (!media) return false;
            if (au.data.length > AU_LIMIT) throw new Error("Video frame too large (" + au.data.length + " bytes).");
            if (au.sps && au.pps && !spsLength) {
                heap.set(au.sps, SPS); spsLength = au.sps.length;
                heap.set(au.pps, PPS); ppsLength = au.pps.length;
            }
            heap.set(au.data, AU);
            var result = media.videoFrame(AU, au.data.length, 0, Math.round(au.pts / 90),
                spsLength ? SPS : 0, spsLength, ppsLength ? PPS : 0, ppsLength, heap);
            return result === 0;
        }

        function audioFrame(packet) {
            if (!media || packet.data.length > 65536) return;
            heap.set(packet.data, AUDIO);
            try { media.audioFrame(AUDIO, packet.data.length, heap); }
            catch (e) { if (++failures <= 3) log("Audio: " + e.message); }
        }

        function close() {
            if (!media) return;
            var m = media; media = null;
            m.close();
        }

        return {
            setup: setup,
            videoFrame: videoFrame,
            audioFrame: audioFrame,
            drainVideo: function () { if (media) { media.drainVideo(); media.finishVideo(); } },
            drainAudio: function () { if (media) media.drainAudio(); },
            close: close,
            stats: function () { return media ? media.stats() : null; },
            mode: function () { return G.__jellyfinSyncGpu ? "synchronous" : "asynchronous"; },
            // Average time from handing a frame to the decoder until it is on screen,
            // measured by the native media module itself (seconds), or null.
            latency: function () {
                if (!media) return null;
                var st = media.stats();
                return st && st.presentAgeSamples >= 8 ? st.presentAgeMs / st.presentAgeSamples / 1000 : null;
            }
        };
    }

    JF.createNativeSink = create;
})();

;
/* Playback diagnostics: per-second measurements, a rolling 5-second view for the
 * stats overlay, and a 30-second recorded report with plain-language findings.
 * Pure logic: the app feeds it counters; nothing here touches the platform. */
(function () {
    "use strict";
    var G = typeof window !== "undefined" ? window : globalThis;
    var JF = G.JF = G.JF || {};

    var RECORD_SECONDS = 30;

    // Cumulative counters from the player and the native media module become
    // per-second deltas. Missing sink counters (non-native sinks) read as 0.
    function create(options) {
        options = options || {};
        var now = options.now || function () { return Date.now(); };
        var recordSeconds = options.recordSeconds || RECORD_SECONDS;
        var buckets = [], recording = null, lastTickAt = null, startedAt = null, primed = false;
        var current = fresh(), previous = null;

        function fresh() { return {ticks: 0, maxGap: 0, gaps50: 0, gaps100: 0, minBuffer: Infinity}; }

        // Called on every frame-loop iteration.
        function tick(buffer) {
            var t = now();
            if (startedAt === null) startedAt = t;
            if (lastTickAt !== null) {
                var gap = t - lastTickAt;
                if (gap > current.maxGap) current.maxGap = gap;
                if (gap > 50) current.gaps50++;
                if (gap > 100) current.gaps100++;
            }
            lastTickAt = t; current.ticks++;
            if (buffer !== undefined && buffer < current.minBuffer) current.minBuffer = buffer;
        }

        // Called once per second with cumulative counters.
        // snapshot: {player: player.stats, sink: media stats or null, slice: ms, buffer, lead, latency, playing}
        function second(snapshot) {
            var p = snapshot.player || {}, s = snapshot.sink || {};
            var cum = {
                fed: p.videoFed || 0, late: p.lateFrames || 0, skipped: p.videoSkipped || 0, stalls: p.stalls || 0,
                audioFed: p.audioFed || 0, audioDropped: p.audioDropped || 0, bytes: p.bytes || 0,
                decoded: s.decoded || 0, presented: s.presented || 0, superseded: s.videoSuperseded || 0,
                expired: (s.videoExpired || 0) + (s.videoBacklogDrops || 0), underruns: s.audioEmptyEpisodes || 0, underrunMs: s.audioEmptyMs || 0,
                decodeMs: s.decodeMs || 0, decodeSamples: s.decodeSamples || 0, gpuMs: s.gpuTransferMs || 0, gpuCount: s.gpuTransfers || 0,
                intervalCount: s.intervalCount || 0, intervalMs: s.intervalMs || 0, intervalSq: s.intervalSquaredMs || 0
            };
            var d = {};
            for (var k in cum) d[k] = previous ? Math.max(0, cum[k] - previous[k]) : 0;
            previous = cum;
            var mean = d.intervalCount ? d.intervalMs / d.intervalCount : 0;
            var variance = d.intervalCount ? Math.max(0, d.intervalSq / d.intervalCount - mean * mean) : 0;
            var bucket = {
                at: Math.round(((now() - (startedAt || now())) / 1000)), playing: !!snapshot.playing,
                ticks: current.ticks, maxGap: Math.round(current.maxGap), gaps50: current.gaps50, gaps100: current.gaps100,
                slice: Math.round(snapshot.slice || 0), buffer: isFinite(current.minBuffer) ? current.minBuffer : (snapshot.buffer || 0),
                fed: d.fed, decoded: d.decoded, shown: d.presented, late: d.late, skipped: d.skipped, stalls: d.stalls,
                superseded: d.superseded, expired: d.expired, audioFed: d.audioFed, audioDropped: d.audioDropped,
                underruns: d.underruns, underrunMs: Math.round(d.underrunMs), kbps: Math.round(d.bytes * 8 / 1000),
                decodeAvg: d.decodeSamples ? d.decodeMs / d.decodeSamples : 0, gpuAvg: d.gpuCount ? d.gpuMs / d.gpuCount : 0,
                intervalMean: mean, intervalJitter: Math.sqrt(variance),
                lead: snapshot.lead, latency: snapshot.latency, bframes: !!(snapshot.player && snapshot.player.bframes)
            };
            current = fresh();
            if (!primed) { primed = true; return null; }                          // first call only primes the deltas
            buckets.push(bucket);
            if (buckets.length > 5) buckets.shift();
            if (recording && bucket.playing) {
                recording.rows.push(bucket);
                if (recording.rows.length >= recordSeconds) { var done = recording; recording = null; if (done.onDone) done.onDone(done.rows); }
            }
            return bucket;
        }

        // Aggregate of the last few seconds for the overlay.
        function recent() {
            if (!buckets.length) return null;
            return summarize(buckets);
        }

        return {
            tick: tick,
            second: second,
            recent: recent,
            record: function (onDone) { if (recording) return false; recording = {rows: [], onDone: onDone}; return true; },
            recording: function () { return recording ? {seconds: recording.rows.length, total: recordSeconds} : null; },
            cancel: function () { recording = null; }
        };
    }

    function summarize(rows) {
        var n = rows.length, sum = function (k) { return rows.reduce(function (a, r) { return a + (r[k] || 0); }, 0); };
        var max = function (k) { return rows.reduce(function (a, r) { return Math.max(a, r[k] || 0); }, 0); };
        var withIntervals = rows.filter(function (r) { return r.intervalMean > 0; });
        var jitters = withIntervals.map(function (r) { return r.intervalJitter; }).sort(function (a, b) { return a - b; });
        return {
            seconds: n, ticks: sum("ticks"), shownFps: sum("shown") / n, decodedFps: sum("decoded") / n, fedFps: sum("fed") / n,
            maxGap: max("maxGap"), gaps50: sum("gaps50"), gaps100: sum("gaps100"), maxSlice: max("slice"),
            minBuffer: rows.reduce(function (a, r) { return Math.min(a, r.buffer); }, Infinity),
            late: sum("late"), skipped: sum("skipped"), stalls: sum("stalls"), superseded: sum("superseded"), expired: sum("expired"),
            underruns: sum("underruns"), audioDropped: sum("audioDropped"), kbps: sum("kbps") / n,
            intervalMean: withIntervals.length ? withIntervals.reduce(function (a, r) { return a + r.intervalMean; }, 0) / withIntervals.length : 0,
            intervalJitter: jitters.length ? jitters[Math.floor(jitters.length / 2)] : 0,
            worstJitter: jitters.length ? jitters[jitters.length - 1] : 0,
            decodeAvg: rows.reduce(function (a, r) { return a + r.decodeAvg; }, 0) / n,
            gpuAvg: rows.reduce(function (a, r) { return a + r.gpuAvg; }, 0) / n,
            lead: rows[n - 1].lead, latency: rows[n - 1].latency,
            bframes: rows.some(function (r) { return r.bframes; })
        };
    }

    // Plain-language findings, most likely cause first. context: {sourceFps, server: TranscodingInfo or null, mode}
    function analyze(sum, context) {
        context = context || {};
        var findings = [], fps = context.sourceFps || (sum.fedFps > 5 ? sum.fedFps : 0);
        var server = context.server;
        if (sum.bframes && (sum.superseded > 0 || (sum.decodedFps > 5 && sum.shownFps < sum.decodedFps * 0.9)))
            findings.push("B-FRAMES IN THE STREAM: the decoder releases B-frame video in pairs and only the newer frame of each pair can be shown. " +
                "The app asks Jellyfin for Baseline H.264 (no B-frames); this stream still has them, so the server may be ignoring the profile request.");
        if (server && server.Framerate && fps && server.Framerate < fps * 0.95)
            findings.push("SERVER TOO SLOW: Jellyfin transcodes at " + server.Framerate.toFixed(1) + " fps, below the video's " + fps.toFixed(2) +
                " fps" + (server.HardwareAccelerationType && server.HardwareAccelerationType !== "none" ? "" : " (no hardware acceleration)") +
                ". Enable hardware transcoding on the server or choose a lower quality.");
        if (sum.stalls > 0 || sum.minBuffer < 1)
            findings.push("STARVING: the buffer fell to " + sum.minBuffer.toFixed(1) + " s" + (sum.stalls ? " and playback paused " + sum.stalls + "x" : "") +
                ". Downloads are not keeping ahead (server speed or network).");
        var blockedTicks = sum.ticks ? sum.gaps50 / sum.ticks : 0;
        if (sum.maxGap > 100 || blockedTicks > 0.02) {
            if (sum.maxSlice >= sum.maxGap * 0.5)
                findings.push("CONSOLE BUSY WITH THE STREAM: the frame loop paused up to " + sum.maxGap + " ms (" + sum.gaps50 +
                    " pauses over 50 ms) while processing downloads (slices up to " + sum.maxSlice + " ms).");
            else
                findings.push("FRAME LOOP BLOCKED ELSEWHERE: pauses up to " + sum.maxGap + " ms (" + sum.gaps50 + " over 50 ms) not explained by downloads (slices " +
                    sum.maxSlice + " ms). Average GPU copy " + sum.gpuAvg.toFixed(1) + " ms in " + (context.mode || "unknown") + " mode.");
        }
        if (fps && sum.decodedFps < fps * 0.9)
            findings.push("DECODER BEHIND: " + sum.decodedFps.toFixed(1) + " decoded frames/s for a " + fps.toFixed(2) + " fps video (average decode " + sum.decodeAvg.toFixed(1) + " ms).");
        if (sum.superseded > 0 || (sum.decodedFps > 5 && sum.shownFps < sum.decodedFps * 0.9))
            findings.push("FRAMES REPLACED BEFORE SHOWN: " + sum.superseded + " decoded frames were overtaken by newer ones (" + sum.shownFps.toFixed(1) +
                " shown/s vs " + sum.decodedFps.toFixed(1) + " decoded/s). Frames are reaching the screen in bursts.");
        if (sum.skipped > 0) findings.push("FELL BEHIND: " + sum.skipped + " frames were skipped to catch up to the next keyframe.");
        if (sum.late > 0) findings.push("LATE FRAMES: " + sum.late + " frames reached the decoder after their time.");
        // A 60 Hz screen can only change picture every 16.7 ms, so a frame rate that does
        // not divide 60 alternates between whole refreshes (24 fps: 2 and 3, i.e. 33.3 and
        // 50 ms, +/- 8.3 ms). Only spacing beyond that built-in variation is a problem.
        var cadence = expectedCadenceJitter(fps);
        if (sum.intervalJitter > cadence + 4)
            findings.push("UNEVEN SPACING: frames appear every " + sum.intervalMean.toFixed(1) + " ms on average, varying by about " + sum.intervalJitter.toFixed(1) +
                " ms (worst second " + sum.worstJitter.toFixed(1) + " ms); the screen's own cadence for this frame rate accounts for " + cadence.toFixed(1) + " ms.");
        if (sum.underruns > 0) findings.push("AUDIO RAN DRY " + sum.underruns + "x, a sign of general overload.");
        if (!findings.length) {
            if (cadence > 1)
                findings.push("DELIVERY LOOKS STEADY. Frames follow the 60 Hz screen's cadence for " + fps.toFixed(3) + " fps (alternating " +
                    "whole refreshes, +/- " + cadence.toFixed(1) + " ms), the normal judder of film on a 60 Hz display.");
            else findings.push("DELIVERY LOOKS STEADY: no stalls, loop pauses, skipped or late frames measured.");
        }
        return findings;
    }

    // Standard deviation of on-screen frame durations when fps frames are shown on a
    // 60 Hz display (each frame lasts a whole number of 16.7 ms refreshes).
    function expectedCadenceJitter(fps) {
        if (!fps || fps <= 0 || fps > 60) return 0;
        var refresh = 1000 / 60, n = 60 / fps, frac = n - Math.floor(n);
        if (frac < 0.01 || frac > 0.99) return 0;
        return refresh * Math.sqrt(frac * (1 - frac));
    }

    function pad(value, width) { value = String(value); while (value.length < width) value = " " + value; return value; }

    // Full text report. info: {version, item, sourceFps, sourceRes, stream, quality, mode, controller, server, startedAt}
    function formatReport(rows, info) {
        var sum = summarize(rows), findings = analyze(sum, {sourceFps: info.sourceFps, server: info.server, mode: info.mode});
        var lines = [];
        lines.push("YAJA (Yet another jellyfin app) " + info.version + " playback report   " + (info.startedAt || ""));
        lines.push("Title:      " + info.item);
        lines.push("Source:     " + (info.sourceRes || "?") + " @ " + (info.sourceFps ? info.sourceFps.toFixed(3) + " fps" : "? fps") + (info.sourceCodec ? " (" + info.sourceCodec + ")" : ""));
        lines.push("Stream:     " + (info.stream || "?") + "   B-frames: " + (sum.bframes ? "yes" : "no") + "   quality setting: " + (info.quality || "?"));
        lines.push("Video path: " + (info.mode || "?") + " presentation   controller: " + (info.controller || "?"));
        var sv = info.server;
        lines.push("Server:     " + (sv ? (sv.VideoCodec || "?") + "/" + (sv.AudioCodec || "?") + " " + (sv.Width || "?") + "x" + (sv.Height || "?") +
            " transcoding at " + (sv.Framerate ? sv.Framerate.toFixed(1) + " fps" : "? fps") + ", hardware: " + (sv.HardwareAccelerationType || "none") +
            (sv.Bitrate ? ", " + Math.round(sv.Bitrate / 1000) + " kbps" : "") : "no transcode info (session not found)"));
        lines.push("");
        lines.push("FINDINGS");
        findings.forEach(function (f, i) { lines.push((i + 1) + ". " + f); });
        lines.push("");
        lines.push("SUMMARY over " + sum.seconds + " s");
        lines.push("  shown " + sum.shownFps.toFixed(1) + " fps, decoded " + sum.decodedFps.toFixed(1) + " fps, fed " + sum.fedFps.toFixed(1) + " fps");
        lines.push("  frame spacing " + sum.intervalMean.toFixed(1) + " ms +/- " + sum.intervalJitter.toFixed(1) + " ms (worst second +/- " + sum.worstJitter.toFixed(1) + " ms)");
        lines.push("  loop pauses: max " + sum.maxGap + " ms, " + sum.gaps50 + " over 50 ms, " + sum.gaps100 + " over 100 ms, of " + sum.ticks + " ticks");
        lines.push("  download slices max " + sum.maxSlice + " ms, " + Math.round(sum.kbps) + " kbps, buffer min " + sum.minBuffer.toFixed(1) + " s");
        lines.push("  late " + sum.late + ", skipped " + sum.skipped + ", stalls " + sum.stalls + ", replaced " + sum.superseded + ", expired " + sum.expired);
        lines.push("  decode avg " + sum.decodeAvg.toFixed(1) + " ms, GPU copy avg " + sum.gpuAvg.toFixed(1) + " ms, lead " + sum.lead + " ms, measured latency " +
            (sum.latency !== null && sum.latency !== undefined ? Math.round(sum.latency * 1000) + " ms" : "?"));
        lines.push("  audio: underruns " + sum.underruns + ", dropped " + sum.audioDropped);
        lines.push("");
        lines.push("PER SECOND");
        lines.push("   s shown dec  fed spacing+-jit  gapMax >50 slice buf  late skip repl undr  kbps");
        rows.forEach(function (r, i) {
            lines.push(pad(i + 1, 4) + pad(r.shown, 6) + pad(r.decoded, 4) + pad(r.fed, 5) + pad(r.intervalMean.toFixed(1), 8) + pad(r.intervalJitter.toFixed(1), 6) +
                pad(r.maxGap, 7) + pad(r.gaps50, 4) + pad(r.slice, 6) + pad(r.buffer.toFixed(1), 5) + pad(r.late, 5) + pad(r.skipped, 5) +
                pad(r.superseded, 5) + pad(r.underruns, 5) + pad(r.kbps, 6));
        });
        return {text: lines.join("\n"), findings: findings, summary: sum};
    }

    JF.diagnostics = {expectedCadenceJitter: expectedCadenceJitter, create: create, summarize: summarize, analyze: analyze, formatReport: formatReport, RECORD_SECONDS: RECORD_SECONDS};
    if (typeof module !== "undefined" && module.exports) module.exports = JF.diagnostics;
})();

;
/* UI toolkit for controller-driven screens: DOM helpers, focus navigation,
 * an on-screen keyboard and a throttled artwork loader. */
/* Credits: the artwork cache limit (8 MiB of data URLs, one download at a time),
 * scrollTop-based scrolling and the absence of CSS animation follow XITRIX's
 * console-tested Moonlight-CivSwitch menus (app/src/ui/game-artwork.js,
 * hosts-screen.js). */
(function () {
    "use strict";
    var G = typeof window !== "undefined" ? window : globalThis;
    var JF = G.JF = G.JF || {};

    // tvOS-like palette: near-black, white focus, system greys.
    var C = JF.colors = {
        bg: "#0c0c0f", panel: "#1c1c1e", card: "#2c2c2e", line: "#3a3a3c", text: "#ffffff",
        dim: "#8e8e93", accent: "#0a84ff", accentDark: "rgba(255,255,255,0.24)", focus: "#ffffff", danger: "#ff453a",
        pill: "rgba(255,255,255,0.13)", shadow: "0 14px 34px rgba(0,0,0,0.65)"
    };

    // Jellyfin blue (#00A4DC, top) to purple (#AA5CC3, bottom): a 1x256 PNG stretched
    // behind cards. An image, not a CSS gradient: images are the display path proven
    // on the console's UI engine.
    JF.gradientUrl = "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAEACAIAAAD9XIvPAAAAeklEQVR42rWTUQrAIAxD097/GDvW7pJ9bMWirS0b+xCi4COJFThOFUBBqOBeAFRIp935sx+aQ4cc0yw4prlyQiYb3l4yU47dZSPv1OGWyYY3l2XrLWEu3ty7lHmTDkMmG94mZuotmLEtk428QYfpbLDh7S8m67zf/+kFvzpkyLtfYU8AAAAASUVORK5CYII=";

    // Clear at the top, fading to 90% black at the bottom: stretched across banners so
    // their text sits on a full-width shade (an image, for the same reason as above).
    JF.scrimUrl = "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAEACAYAAAByPhyYAAAAQ0lEQVR42u2LMQ4AMAgCEf//584dNCJLhzoQgQP49+QFAeCWqOzy022Z9YUNU4EHC+6yqR0UEkIXsa2U9YWE2Ite8gCKLAJD2iqbZQAAAABJRU5ErkJggg==";

    // The YAJA app icon (also the launcher tile), shown on the splash screen.
    JF.appIconUrl = "data:image/png;base64,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";

    function el(tag, parent, style, text) {
        var node = document.createElement(tag);
        if (style) for (var k in style) node.style[k] = style[k];
        if (text !== undefined && text !== null) node.textContent = text;
        if (parent) parent.appendChild(node);
        return node;
    }

    function clear(node) { while (node.firstChild) node.removeChild(node.firstChild); }

    function formatTime(seconds) {
        seconds = Math.max(0, Math.floor(seconds || 0));
        var h = Math.floor(seconds / 3600), m = Math.floor(seconds / 60) % 60, s = seconds % 60;
        return (h ? h + ":" + (m < 10 ? "0" : "") : "") + m + ":" + (s < 10 ? "0" : "") + s;
    }

    // rows: [[{el, activate, kind}]]. Focus moves within and between rows.
    // No CSS transforms or transitions: Civ VII's UI engine is only known to be
    // stable with plain layout plus scrollTop scrolling (as Moonlight uses).
    function createNavigator(viewport) {
        var rows = [], row = 0, col = 0, onMove = null;
        function item() { return rows[row] && rows[row][col]; }
        function paint(target, active) {
            if (!target || !target.el) return;
            var s = target.el.style;
            // tvOS focus without its scaling animation (animations are unstable on the
            // console's UI engine): the focused tile is at full brightness with a white
            // edge and a lifted shadow; the others sit slightly dimmed.
            if (target.kind === "card") {
                s.outline = active ? "4px solid " + C.focus : "4px solid transparent";
                s.boxShadow = active ? C.shadow : "none";
                s.opacity = active ? "1" : "0.72";
                s.backgroundColor = "transparent";
                if (target.label) target.label.style.color = target.labelColors ? (active ? target.labelColors.on : target.labelColors.off) : (active ? C.text : C.dim);
            } else {
                s.backgroundColor = active ? C.focus : (target.base || C.pill);
                s.color = active ? "#000000" : C.text;
                s.boxShadow = active ? "0 10px 26px rgba(0,0,0,0.55)" : "none";
                s.borderColor = active ? C.focus : "transparent";
            }
        }
        function rect(node) { try { return node && node.getBoundingClientRect ? node.getBoundingClientRect() : null; } catch (e) { return null; } }
        // Vertical scrolling only follows row changes: moving sideways never scrolls
        // (revealing a card in a sliding row could otherwise report position 0 before
        // the engine lays it out, which used to send the page to the top). Rows are
        // measured by their always-visible row element, relative to the viewport.
        function scroll(rowChanged) {
            var target = item(); if (!target) return;
            if (row === 0) { viewport.scrollTop = 0; return; }
            if (!rowChanged) return;
            var view = viewport.clientHeight || 0; if (!view) return;
            var anchor = target.rowEl || target.el, TOP = 64, BOTTOM = 24;
            var a = rect(anchor), v = rect(viewport);
            if (a && v && a.height > 0) {
                var top = a.top - v.top, bottom = a.bottom - v.top;
                if (top < TOP) viewport.scrollTop = Math.max(0, viewport.scrollTop - (TOP - top));
                else if (bottom > view - BOTTOM) viewport.scrollTop = viewport.scrollTop + (bottom - (view - BOTTOM));
                return;
            }
            // Fallback: layout offsets, only once the row has a size.
            var off = 0, n = anchor, guard = 0, height = anchor.offsetHeight || 0;
            if (!height) return;
            while (n && n !== viewport && guard++ < 32) { off += n.offsetTop || 0; n = n.offsetParent; }
            if (off - TOP < viewport.scrollTop) viewport.scrollTop = Math.max(0, off - TOP);
            else if (off + height + BOTTOM > viewport.scrollTop + view) viewport.scrollTop = off + height + BOTTOM - view;
        }
        function set(r, c, force) {
            if (!rows.length) return;
            var previousRow = row;
            paint(item(), false);
            row = Math.max(0, Math.min(rows.length - 1, r));
            col = Math.max(0, Math.min(rows[row].length - 1, c));
            paint(item(), true);
            if (onMove) onMove(item(), row, col);       // may reveal the item (sliding rows) before scrolling to it
            scroll(force || row !== previousRow);
        }
        return {
            setRows: function (next, keep) {
                rows = next.filter(function (r) { return r && r.length; });
                rows.forEach(function (r) { r.forEach(function (i) { paint(i, false); }); });
                if (!keep) { row = 0; col = 0; viewport.scrollTop = 0; }
                set(row, col, true);
            },
            // An item may define moves[dir]() -> {row, col} (or null to stay put), for
            // layouts that are not a plain grid, such as side-by-side panes.
            move: function (dir) {
                if (!rows.length) return;
                var from = item();
                if (from && from.moves && from.moves[dir]) { var to = from.moves[dir](); if (to) set(to.row, to.col); return; }
                if (dir === "up") set(row - 1, col);
                else if (dir === "down") set(row + 1, col);
                else if (dir === "left") set(row, col - 1);
                else if (dir === "right") set(row, col + 1);
            },
            activate: function () { var i = item(); if (i && i.activate) i.activate(); },
            current: item,
            rows: function () { return rows; },
            position: function () { return {row: row, col: col}; },
            focus: function (r, c) { set(r, c, true); },
            onMove: function (fn) { onMove = fn; }
        };
    }

    // Rounded "pill" button, white with black text when focused.
    function button(parent, label, activate, style) {
        style = Object.assign({}, style || {});
        var base = style.base || C.pill; delete style.base;
        var b = el("div", parent, Object.assign({
            padding: "14px 30px", margin: "7px 0", borderRadius: "30px", border: "2px solid transparent",
            backgroundColor: base, color: C.text, fontSize: "23px", fontWeight: "bold", minWidth: "320px", boxSizing: "border-box"
        }, style), label);
        return {el: b, activate: activate, kind: "button", base: base};
    }

    // Horizontal pill for action rows and tab bars.
    function pill(parent, label, activate, style) {
        return button(parent, label, activate, Object.assign({minWidth: "0", margin: "0 14px 0 0", textAlign: "center", padding: "14px 34px"}, style || {}));
    }

    // Modal keyboard: resolves with the text, or null when cancelled.
    function createKeyboard(host) {
        var active = null;
        // Pages: 0 lowercase, 1 uppercase, 2 symbols.
        var layouts = [
            ["1234567890", "qwertyuiop", "asdfghjkl-", "zxcvbnm._@"],
            ["1234567890", "QWERTYUIOP", "ASDFGHJKL:", "ZXCVBNM,/!"],
            ["1234567890", "!@#$%^&*()`~", "-_=+[]{};:", "'\",.<>/?\\|"]
        ];
        function open(title, value, opts) {
            opts = opts || {};
            if (active) active.finish(null);
            return new Promise(function (resolve) {
                var text = String(value || ""), page = 0, r = 1, c = 0;
                var overlay = el("div", host, {position: "fixed", left: "0", top: "0", right: "0", bottom: "0", backgroundColor: "rgba(0,0,0,0.78)", zIndex: "50", display: "flex", alignItems: "center", justifyContent: "center"});
                var panel = el("div", overlay, {backgroundColor: "rgba(28,28,30,0.97)", borderRadius: "24px", padding: "30px", width: "980px", boxShadow: C.shadow});
                el("div", panel, {fontSize: "22px", color: C.dim, marginBottom: "10px"}, title);
                var field = el("div", panel, {fontSize: "34px", padding: "14px 20px", backgroundColor: "rgba(255,255,255,0.08)", borderRadius: "14px", minHeight: "44px", marginBottom: "18px", wordBreak: "break-all"});
                var grid = el("div", panel, {});
                el("div", panel, {fontSize: "18px", color: C.dim, marginTop: "14px"}, "A: type   B: cancel   X: delete   Y: shift   ZR: symbols   +: done");
                var keys = [];
                function render() {
                    field.textContent = (opts.secret ? text.replace(/./g, "*") : text) + "_";
                    clear(grid); keys = [];
                    var set = layouts[page];
                    set.forEach(function (line) {
                        var rowEl = el("div", grid, {display: "flex", justifyContent: "center"}), rowKeys = [];
                        line.split("").forEach(function (ch) { rowKeys.push({el: el("div", rowEl, keyStyle(56)), value: ch}); rowKeys[rowKeys.length - 1].el.textContent = ch; });
                        keys.push(rowKeys);
                    });
                    if (opts.quickKeys && opts.quickKeys.length) {
                        var quick = el("div", grid, {display: "flex", justifyContent: "center"}), quickKeys = [];
                        opts.quickKeys.forEach(function (q) { quickKeys.push({el: el("div", quick, keyStyle(Math.max(140, q.length * 18 + 40)), q), value: "quick:" + q}); });
                        keys.push(quickKeys);
                    }
                    var last = el("div", grid, {display: "flex", justifyContent: "center"}), specials = [];
                    [[page === 1 ? "SHIFT" : "Shift", "shift", 140], [page === 2 ? "abc" : "#+=", "symbols", 130], ["Space", " ", 260], ["Delete", "del", 140], ["Done", "done", 170]].forEach(function (k) {
                        var node = el("div", last, keyStyle(k[2]), k[0]); specials.push({el: node, value: k[1]});
                    });
                    keys.push(specials);
                    r = Math.min(r, keys.length - 1); c = Math.min(c, keys[r].length - 1); paint();
                }
                function keyStyle(width) { return {width: width + "px", height: "56px", lineHeight: "56px", margin: "5px", textAlign: "center", fontSize: "26px", borderRadius: "12px", backgroundColor: C.pill, color: C.text}; }
                function paint() {
                    keys.forEach(function (line, i) { line.forEach(function (k, j) {
                        var on = i === r && j === c;
                        k.el.style.backgroundColor = on ? C.focus : C.pill; k.el.style.color = on ? "#000000" : C.text;
                    }); });
                }
                function press(v) {
                    if (v === "shift") { page = page === 0 ? 1 : 0; render(); return; }
                    if (v === "symbols") { page = page === 2 ? 0 : 2; render(); return; }
                    if (v === "del") { text = text.slice(0, -1); render(); return; }
                    if (v === "done") { finish(text); return; }
                    if (v.indexOf("quick:") === 0) {
                        var insert = v.slice(6), limit = opts.maxLength || 128;
                        // A scheme replaces any scheme already typed instead of stacking.
                        if (/^[a-z]+:\/\/$/i.test(insert)) text = insert + text.replace(/^[a-z][a-z0-9+.-]*:\/\//i, "");
                        else text += insert;
                        text = text.slice(0, limit); render(); return;
                    }
                    if (text.length < (opts.maxLength || 128)) text += v;
                    render();
                }
                function finish(result) {
                    if (overlay.parentNode) overlay.parentNode.removeChild(overlay);
                    active = null; resolve(result);
                }
                active = {
                    finish: finish,
                    action: function (name) {
                        if (name === "up") { r = Math.max(0, r - 1); c = Math.min(c, keys[r].length - 1); paint(); }
                        else if (name === "down") { r = Math.min(keys.length - 1, r + 1); c = Math.min(c, keys[r].length - 1); paint(); }
                        else if (name === "left") { c = Math.max(0, c - 1); paint(); }
                        else if (name === "right") { c = Math.min(keys[r].length - 1, c + 1); paint(); }
                        else if (name === "accept") press(keys[r][c].value);
                        else if (name === "cancel") finish(null);
                        else if (name === "menu") press("del");
                        else if (name === "options") press("shift");
                        else if (name === "zr") press("symbols");
                        else if (name === "plus") finish(text);
                        else if (name.length === 6 && name.indexOf("char:") === 0) press(name.slice(5));
                        else if (name === "backspace") press("del");
                    }
                };
                render();
            });
        }
        return {open: open, active: function () { return !!active; }, action: function (n) { if (active) active.action(n); }};
    }

    // Artwork: one download at a time, an LRU cache bounded by the size of the
    // data URLs it holds (8 MiB, as Moonlight does), and images released from
    // <img> elements that are far from the focus so the engine can free them.
    function createArtwork(fetchImage, log, options) {
        options = options || {};
        var budget = options.budget || 8 * 1048576;
        var cache = new Map(), bytes = 0, failed = new Set(), queue = [], running = false, generation = 0;
        var shown = new Map();                          // img node -> key currently displayed
        function remember(key, url) {
            var size = url.length * 2;
            if (size > budget) return;
            if (cache.has(key)) { bytes -= cache.get(key).length * 2; cache.delete(key); }
            while (bytes + size > budget && cache.size) { var oldest = cache.keys().next().value; bytes -= cache.get(oldest).length * 2; cache.delete(oldest); }
            cache.set(key, url); bytes += size;
        }
        function display(node, key, url) { node.src = url; node.style.display = ""; shown.set(node, key); }
        function pump() {
            if (running) return;
            var job;
            while (queue.length) { job = queue.shift(); if (job.generation === generation && job.wanted() && !cache.has(job.key) && !failed.has(job.key)) break; job = null; }
            if (!job) return;
            running = true;
            fetchImage(job.id, job.tag, job.height, job.kind).then(function (url) {
                if (!url) { failed.add(job.key); return; }
                remember(job.key, url);
                if (job.generation === generation && job.wanted()) display(job.node, job.key, url);
            }).catch(function (e) { failed.add(job.key); log("Artwork: " + e.message); })
              .then(function () { running = false; pump(); });
        }
        return {
            show: function (node, id, tag, height, kind) {
                if (!id || !tag || !node) return;
                kind = kind || "Primary";
                var key = id + ":" + kind + ":" + tag + ":" + height;
                if (shown.get(node) === key) return;
                if (cache.has(key)) { var url = cache.get(key); cache.delete(key); cache.set(key, url); display(node, key, url); return; }
                if (failed.has(key) || queue.some(function (j) { return j.node === node && j.key === key; })) return;
                queue.push({key: key, id: id, tag: tag, height: height, kind: kind, node: node, generation: generation,
                    wanted: function () { return !!node.parentNode && node.__jfWanted !== false; }});
                pump();
            },
            release: function (node) {
                if (!node || !shown.has(node)) return;
                node.removeAttribute("src"); node.style.display = "none"; shown.delete(node);
            },
            reset: function () { generation++; queue = []; shown.clear(); },
            idle: function () { return !running && !queue.length; },
            stats: function () { return {cached: cache.size, bytes: bytes, queued: queue.length, shown: shown.size, failed: failed.size}; }
        };
    }

    JF.ui = {el: el, clear: clear, formatTime: formatTime, createNavigator: createNavigator, button: button, pill: pill, createKeyboard: createKeyboard, createArtwork: createArtwork};
})();

;
/* YAJA (Yet another jellyfin app): screens, input, playback control and lifetime.
 * JF.startApp(platform) -> {close, lifetime}. platform = {transport, storage, createSink, sleep, close}. */
/* Credits: the app lifetime contract (window.__civswitchApp, the lifetime promise,
 * full cleanup on close) and engine input handling (InputAction names, nav-move
 * repeat) follow XITRIX's CivHomebrew launcher and its Hello example. Reading X/Y/L/R
 * natively follows CivHomebrew's controller polling. Showing decoded video as a
 * live:/ background image follows Moonlight-CivSwitch's stream screen (XITRIX).
 * The Jellyfin REST routes come from the Jellyfin server source; this client is
 * independent and not affiliated with the Jellyfin project. */
(function () {
    "use strict";
    var G = typeof window !== "undefined" ? window : globalThis;
    var JF = G.JF = G.JF || {};
    var VERSION = "1.0.0";
    var QUALITIES = [
        {label: "720p - 4 Mbps (recommended)", maxWidth: 1280, maxHeight: 720, videoBitrate: 4000000},
        {label: "720p - 6 Mbps", maxWidth: 1280, maxHeight: 720, videoBitrate: 6000000},
        {label: "1080p - 8 Mbps", maxWidth: 1920, maxHeight: 1080, videoBitrate: 8000000},
        {label: "1080p - 12 Mbps", maxWidth: 1920, maxHeight: 1080, videoBitrate: 12000000},
        {label: "480p - 2 Mbps (slow network)", maxWidth: 854, maxHeight: 480, videoBitrate: 2000000}
    ];

    function startApp(platform) {
        var ui = JF.ui, C = JF.colors, el = ui.el;
        var closed = false, finish, lifetime = new Promise(function (r) { finish = r; });
        var removers = [], timers = new Set(), diagnostics = [];
        var log = JF.log = function (message) {
            diagnostics.push(new Date().toISOString().slice(11, 19) + "  " + String(message).slice(0, 300));
            if (diagnostics.length > 80) diagnostics.shift();
        };

        // ---- Settings ---------------------------------------------------------
        var settings = null;
        try { settings = platform.storage.load(); } catch (e) { log("Settings: " + e.message); }
        settings = Object.assign({server: "", host: "", port: 8096, deviceId: "", token: null, userId: null, userName: "", quality: 0, autoplay: true, tls: false, basePath: "", tlsVerify: true, tz: "auto"}, settings || {});
        // Older versions stored only host and port.
        if (!settings.server && settings.host) settings.server = "http://" + settings.host + ":" + settings.port;
        // Versions before 0.1.9 stored no scheme or path; derive them from the address.
        try { if (settings.server) { var parsed0 = JF.jellyfin.parseServerAddress(settings.server); settings.tls = parsed0.tls; settings.basePath = parsed0.basePath; } } catch (e) {}
        if (platform.setTlsVerify) platform.setTlsVerify(settings.tlsVerify);
        if (!settings.deviceId) settings.deviceId = "civswitch-" + Math.random().toString(16).slice(2) + Date.now().toString(16);
        function persist() { try { platform.storage.save(settings); } catch (e) { log("Saving settings failed: " + e.message); } }

        var api = null;
        function makeApi() {
            api = JF.jellyfin.create({transport: platform.transport, host: settings.host, port: settings.port, tls: settings.tls, basePath: settings.basePath, deviceId: settings.deviceId,
                deviceName: "Nintendo Switch (CivSwitch)", client: "YAJA", version: VERSION, token: settings.token, userId: settings.userId});
        }

        // ---- Layout -----------------------------------------------------------
        var root = el("div", document.body, {position: "fixed", left: "0", top: "0", right: "0", bottom: "0", backgroundColor: C.bg, color: C.text,
            fontFamily: "BodyFont, Arial, sans-serif", zIndex: "1", overflow: "hidden", outline: "none"});
        root.id = "jellyfin-civswitch"; root.tabIndex = -1;
        var header = el("div", root, {position: "absolute", left: "0", right: "0", top: "0", height: "84px", display: "flex", alignItems: "center", padding: "0 48px", boxSizing: "border-box", backgroundColor: C.bg, zIndex: "5"});
        var logo = el("div", header, {fontSize: "28px", fontWeight: "bold", color: C.text, marginRight: "26px"}, "YAJA");
        var titleEl = el("div", header, {fontSize: "24px", color: C.dim, flex: "1", whiteSpace: "nowrap", overflow: "hidden", textOverflow: "ellipsis"});
        var hintEl = el("div", header, {fontSize: "17px", color: C.dim});
        var viewport = el("div", root, {position: "absolute", left: "0", right: "0", top: "84px", bottom: "56px", overflowY: "auto", overflowX: "hidden", zIndex: "1"});
        var content = el("div", viewport, {padding: "28px 48px", boxSizing: "border-box"});
        var statusEl = el("div", root, {position: "absolute", left: "0", right: "0", bottom: "0", height: "56px", lineHeight: "56px", padding: "0 48px", fontSize: "19px", color: C.dim, backgroundColor: C.bg, whiteSpace: "nowrap", overflow: "hidden", textOverflow: "ellipsis", zIndex: "2"});
        // A screen may show artwork behind everything (the TV show page uses its backdrop).
        var backdrop = el("div", root, {position: "absolute", left: "0", top: "0", right: "0", bottom: "0", zIndex: "0", overflow: "hidden", display: "none"});
        var backdropImg = el("img", backdrop, {position: "absolute", left: "0", top: "0", display: "none"});
        var backdropDim = el("div", backdrop, {position: "absolute", left: "0", top: "0", right: "0", bottom: "0", backgroundColor: "rgba(12,12,15,0.7)"});
        var backdropFade = el("img", backdrop, {position: "absolute", left: "0", top: "0", display: "none"});
        backdropFade.src = JF.scrimUrl;
        // options.dim: darkening over the whole image; options.fade: the bottom fade for text.
        function setBackdrop(pick, options) {
            options = options || {};
            if (!pick) { backdrop.style.display = "none"; artwork.release(backdropImg); header.style.backgroundColor = C.bg; statusEl.style.backgroundColor = C.bg; return; }
            backdropDim.style.backgroundColor = "rgba(12,12,15," + (options.dim !== undefined ? options.dim : 0.7) + ")";
            var sw = root.clientWidth || viewport.clientWidth || 1280, sh = root.clientHeight || 720;
            backdropFade.style.display = options.fade ? "" : "none";
            backdropFade.style.width = sw + "px"; backdropFade.style.height = sh + "px";
            var w = root.clientWidth || viewport.clientWidth || 1280, h = Math.round(w * 9 / 16), screenH = root.clientHeight || 720;
            if (h < screenH) { h = screenH; w = Math.round(h * 16 / 9); }               // cover the whole screen
            backdropImg.style.width = w + "px"; backdropImg.style.height = h + "px";
            backdropImg.style.left = Math.round(((root.clientWidth || w) - w) / 2) + "px";
            backdrop.style.display = "";
            header.style.backgroundColor = "transparent"; statusEl.style.backgroundColor = "transparent";
            artwork.show(backdropImg, pick.id, pick.tag, 540, pick.kind);
        }
        // Splash: the app icon while Home loads behind it (including visible artwork), so
        // the first screen appears complete. Shown at least 0.7 s, never more than 12 s.
        // The fade steps opacity from script (no CSS animation, see ui-kit).
        var splash = el("div", root, {position: "absolute", left: "0", top: "0", right: "0", bottom: "0", zIndex: "40", backgroundColor: C.bg,
            display: "flex", flexDirection: "column", alignItems: "center", justifyContent: "center"});
        var splashIcon = el("img", splash, {width: "240px", height: "240px"});
        splashIcon.src = JF.appIconUrl;
        el("div", splash, {fontSize: "26px", fontWeight: "bold", color: C.text, marginTop: "18px"}, "Yet another jellyfin app");
        var splashNote = el("div", splash, {fontSize: "18px", color: C.dim, marginTop: "14px", minHeight: "24px"}, "");
        var splashShownAt = Date.now(), splashDone = false;
        function hideSplash() {
            if (splashDone) return;
            splashDone = true;
            var wait = Math.max(0, 700 - (Date.now() - splashShownAt)), step = 0;
            function fade() {
                step++;
                splash.style.opacity = String(Math.max(0, 1 - step / 6));
                if (step < 6) { var t = setTimeout(fade, 45); timers.add(t); }
                else if (splash.parentNode) splash.parentNode.removeChild(splash);
            }
            var t0 = setTimeout(fade, wait); timers.add(t0);
        }
        // Home is ready: wait for the visible artwork (at most 2.5 s more), then reveal it.
        function homeReady() {
            if (splashDone) return;
            var until = Date.now() + 2500;
            (function check() {
                if (artwork.idle() || Date.now() > until) { hideSplash(); return; }
                var t = setTimeout(check, 100); timers.add(t);
            })();
        }
        var splashLimit = setTimeout(hideSplash, 12000); timers.add(splashLimit);
        var nav = ui.createNavigator(viewport);
        var keyboard = ui.createKeyboard(root);
        var artwork = ui.createArtwork(function (id, tag, h, kind) { return api.fetchImage(id, kind || "Primary", tag, h); }, log);

        function status(text, isError) { statusEl.textContent = text || ""; statusEl.style.color = isError ? C.danger : C.dim; if (text) log((isError ? "Error: " : "") + text); }

        // ---- Screen stack -------------------------------------------------------
        var stack = [], current = null, busy = false;
        function show(screen, replace) {
            if (current && current.leave) current.leave();
            if (replace && stack.length) stack.pop();
            stack.push(screen); render(screen);
        }
        function back() {
            if (stack.length <= 1) return false;
            if (current && current.leave) current.leave();
            stack.pop(); render(stack[stack.length - 1], true);
            return true;
        }
        function render(screen, restoring) {
            setBackdrop(null);
            screen.restoreTo = restoring && screen.saved ? {row: screen.saved.row, col: screen.saved.col} : null;
            current = screen; artwork.reset();
            ui.clear(content); viewport.scrollTop = 0;
            titleEl.textContent = screen.title || ""; hintEl.textContent = screen.hint || "A: select   B: back";
            header.style.display = viewport.style.display = statusEl.style.display = screen.fullscreen ? "none" : "";
            var rows = screen.build(content) || [];
            nav.setRows(rows, false);
            if (screen.restoreTo) nav.focus(screen.restoreTo.row, screen.restoreTo.col);
            else if (screen.initialFocus) nav.focus(screen.initialFocus.row, screen.initialFocus.col);
        }
        nav.onMove(function (item, row, col) { if (current) current.saved = {row: row, col: col}; if (item && item.onFocus) item.onFocus(); updateArtwork(row); });
        // Only rows near the focus hold artwork; the rest give their images back.
        function updateArtwork(focusRow) {
            var rows = nav.rows();
            for (var r = 0; r < rows.length; r++) {
                for (var i = 0; i < rows[r].length; i++) {
                    var card = rows[r][i];
                    if (!card.img) continue;
                    var near = r >= focusRow - 1 && r <= focusRow + 2 && card.el.style.display !== "none";
                    card.img.__jfWanted = near;
                    if (near) artwork.show(card.img, card.imageId, card.imageTag, card.imageHeight, card.imageKind);
                    else artwork.release(card.img);
                }
            }
        }

        function failed(e) {
            hideSplash();
            status(e.message || String(e), true);
            if (e.status === 401 && settings.token) { settings.token = null; settings.userId = null; persist(); makeApi(); stack = [connectScreen()]; show(loginScreen()); }
        }
        // User-triggered actions (connect, sign in): one at a time, input blocked meanwhile.
        async function guard(label, fn) {
            if (busy) return;
            busy = true; status(label);
            try { await fn(); if (statusEl.textContent === label) status(""); }
            catch (e) { failed(e); }
            finally { busy = false; }
        }
        // Screen data loads: never blocked by an action that opened the screen.
        async function load(label, fn) {
            status(label);
            try { await fn(); if (statusEl.textContent === label) status(""); }
            catch (e) { failed(e); }
        }

        function heading(parent, text) { return el("div", parent, {fontSize: "27px", fontWeight: "bold", color: C.text, margin: "22px 0 8px"}, text); }

        // Poster cards in fixed rows sized to the screen; returns one focus row per line.
        function columns(landscape) {
            var width = viewport.clientWidth || 1280;
            if (landscape) return width >= 1700 ? 4 : width >= 1000 ? 3 : 2;
            return width >= 1700 ? 6 : width >= 1300 ? 5 : width >= 950 ? 4 : 3;
        }
        // options.landscape: 16:9 cards (Home); otherwise 2:3 posters (libraries).
        function cardGrid(parent, items, onPick, maxRows, options) {
            var landscape = !!(options && options.landscape);
            var cols = columns(landscape), width = viewport.clientWidth || 1280, gap = 24;
            var w = Math.max(150, Math.min(landscape ? 400 : 240, Math.floor((width - 96 - gap * (cols - 1)) / cols)));
            // options.gradient: a blue-to-purple frame around the artwork (Home rows).
            var gradient = !!(options && options.gradient), pad = gradient ? 10 : 0;
            var aw = w - pad * 2, h = landscape ? Math.round(aw * 9 / 16) : Math.round(aw * 1.5);
            var cardHeight = h + pad * 2 + 60;
            var rows = [], carousel = !!(options && options.carousel);
            var counter = carousel && items.length > cols ? el("div", parent, {fontSize: "17px", color: C.dim, margin: "-6px 0 2px"}) : null;
            for (var start = 0; start < items.length && (!maxRows || rows.length < maxRows); start += cols) {
                var line = el("div", parent, {display: "flex", padding: "8px 4px"});
                var slice = carousel ? items : items.slice(start, start + cols);
                rows.push(slice.map(function (it) {
                    var card = el("div", line, gradient
                        ? {position: "relative", width: w + "px", height: cardHeight + "px", boxSizing: "border-box", padding: pad + "px", marginRight: gap + "px", flexShrink: "0", borderRadius: "18px", overflow: "hidden", outline: "4px solid transparent"}
                        : {width: w + "px", marginRight: gap + "px", flexShrink: "0", borderRadius: "16px", padding: "0", outline: "4px solid transparent"});
                    if (gradient) { var shade = el("img", card, {position: "absolute", left: "0", top: "0", width: w + "px", height: cardHeight + "px"}); shade.src = JF.gradientUrl; }
                    var art = el("div", card, {position: "relative", width: aw + "px", height: h + "px", borderRadius: gradient ? "12px" : "14px", backgroundColor: C.card, overflow: "hidden"});
                    el("div", art, {position: "absolute", left: "10px", right: "10px", top: "40%", textAlign: "center", fontSize: "18px", color: C.dim}, it.Name);
                    var pick = landscape ? JF.jellyfin.landscapeImage(it) : null;
                    var own = it.ImageTags && it.ImageTags.Primary;
                    if (!landscape) pick = own ? {id: it.Id, kind: "Primary", tag: it.ImageTags.Primary, fill: true}
                        : (it.SeriesPrimaryImageTag ? {id: it.SeriesId, kind: "Primary", tag: it.SeriesPrimaryImageTag, fill: true} : null);
                    // A tall poster inside a wide card is centred at its own shape, never stretched.
                    var iw = pick && !pick.fill ? Math.round(h * 2 / 3) : aw;
                    var img = el("img", art, {position: "absolute", left: Math.round((aw - iw) / 2) + "px", top: "0", width: iw + "px", height: h + "px", display: "none"});
                    var progress = it.UserData && it.RunTimeTicks && it.UserData.PlaybackPositionTicks ? it.UserData.PlaybackPositionTicks / it.RunTimeTicks : 0;
                    if (progress > 0.01) {
                        var track = el("div", art, {position: "absolute", left: "14px", right: "14px", bottom: "12px", height: "5px", borderRadius: "3px", backgroundColor: "rgba(255,255,255,0.3)", overflow: "hidden"});
                        el("div", track, {height: "100%", width: Math.min(100, progress * 100) + "%", backgroundColor: C.text});
                    }
                    var episodeTitles = !!(options && options.episodeTitles);
                    var name = episodeTitles ? (it.IndexNumber != null ? it.IndexNumber + ". " : "") + it.Name : (it.Type === "Episode" && it.SeriesName ? it.SeriesName : it.Name);
                    var label = el("div", card, {position: gradient ? "relative" : "static", fontSize: "19px", fontWeight: "bold", marginTop: "10px", color: gradient ? "#ffffff" : C.dim, whiteSpace: "nowrap", overflow: "hidden", textOverflow: "ellipsis", width: aw + "px"}, name);
                    var sub = episodeTitles ? (it.RunTimeTicks ? Math.round(it.RunTimeTicks / 600000000) + " min" : "") + (it.UserData && it.UserData.Played ? "  -  Watched" : "")
                        : (it.Type === "Episode" ? "S" + (it.ParentIndexNumber || "?") + ":E" + (it.IndexNumber || "?") + " - " + it.Name : (it.ProductionYear || ""));
                    el("div", card, {position: gradient ? "relative" : "static", fontSize: "16px", color: gradient ? "rgba(255,255,255,0.85)" : C.dim, whiteSpace: "nowrap", overflow: "hidden", textOverflow: "ellipsis", width: aw + "px"}, String(sub));
                    return {el: card, rowEl: line, kind: "card", img: img, label: label, labelColors: gradient ? {on: "#ffffff", off: "rgba(255,255,255,0.92)"} : null, activate: function () { onPick(it); },
                        imageId: pick && pick.id, imageTag: pick && pick.tag, imageKind: pick ? pick.kind : "Primary", imageHeight: h};
                }));
                if (carousel) { windowRow(rows[rows.length - 1], cols, counter); break; }
            }
            return rows;
        }
        // Shows `cols` cards of a long row at a time and slides the window to follow
        // the focus. Plain show/hide only: no transforms or horizontal scrolling.
        function windowRow(row, cols, counter) {
            var first = 0;
            function apply() {
                for (var i = 0; i < row.length; i++) row[i].el.style.display = i >= first && i < first + cols ? "" : "none";
                if (counter) counter.textContent = (first > 0 ? "<  " : "") + (first + 1) + "-" + Math.min(first + cols, row.length) + " of " + row.length + (first + cols < row.length ? "  >" : "");
            }
            row.forEach(function (item, index) {
                item.onFocus = function () {
                    if (index < first) first = index;
                    else if (index >= first + cols) first = index - cols + 1;
                    apply();
                };
            });
            apply();
        }
        function addRows(target, more) { for (var i = 0; i < more.length; i++) target.push(more[i]); }

        // ---- Tabs: Home, each video library, Settings (L / R cycle through them) ----
        var tabViews = [], liveTvEnabled = false;
        function tabList() {
            return [{key: "home", name: "Home"}].concat(tabViews.map(function (v) { return {key: v.Id, name: v.Name, view: v}; }),
                liveTvEnabled ? [{key: "livetv", name: "Live TV"}] : [], [{key: "settings", name: "Settings"}]);
        }
        function tabBar(container, activeKey) {
            return tabList().map(function (t) {
                return ui.pill(container, t.name, function () { goTab(t.key); },
                    {base: t.key === activeKey ? C.accentDark : C.pill, fontSize: "21px", padding: "10px 28px"});
            });
        }
        // Tabs sit directly above Home in the screen stack, so B from any tab returns Home.
        function goTab(key) {
            var homeIndex = -1;
            for (var i = stack.length - 1; i >= 0; i--) if (stack[i].isHome) { homeIndex = i; break; }
            if (key === "home") {
                if (homeIndex < 0) { show(homeScreen()); return; }
                if (current && current.leave) current.leave();
                stack.length = homeIndex + 1; render(stack[homeIndex], true); return;
            }
            var target;
            if (key === "settings") target = settingsScreen();
            else if (key === "livetv") target = guideScreen();
            else {
                var view = tabViews.filter(function (v) { return v.Id === key; })[0];
                if (!view) return;
                target = libraryScreen(view, {tab: true});
            }
            if (current && current.leave) current.leave();
            if (homeIndex >= 0) stack.length = homeIndex + 1;
            stack.push(target); render(target);
        }
        function cycleTab(delta) {
            var tabs = tabList(), key = current && current.tabKey, at = -1;
            for (var k = 0; k < tabs.length; k++) if (tabs[k].key === key) at = k;
            if (at >= 0) goTab(tabs[(at + delta + tabs.length) % tabs.length].key);
        }

        // Wide artwork for a hero banner: backdrops first, then thumbs, then 16:9 stills.
        function heroImage(it) {
            if (it.BackdropImageTags && it.BackdropImageTags.length) return {id: it.Id, kind: "Backdrop", tag: it.BackdropImageTags[0]};
            if (it.ParentBackdropItemId && it.ParentBackdropImageTags && it.ParentBackdropImageTags.length) return {id: it.ParentBackdropItemId, kind: "Backdrop", tag: it.ParentBackdropImageTags[0]};
            return JF.jellyfin.landscapeImage(it);
        }
        function runtime(ticks) {
            var m = Math.round((ticks || 0) / 600000000);
            return m >= 60 ? Math.floor(m / 60) + " h " + (m % 60 ? m % 60 + " min" : "") : m + " min";
        }
        function metaLine(it) {
            var bits = [];
            if (it.Type === "Episode") bits.push("S" + (it.ParentIndexNumber || "?") + " - E" + (it.IndexNumber || "?"));
            if (it.ProductionYear && it.Type !== "Episode") bits.push(it.ProductionYear);
            if (it.RunTimeTicks) bits.push(runtime(it.RunTimeTicks).trim());
            if (it.OfficialRating) bits.push(it.OfficialRating);
            var pos = it.UserData && it.UserData.PlaybackPositionTicks;
            if (pos && it.RunTimeTicks && pos < it.RunTimeTicks) bits.push(runtime(it.RunTimeTicks - pos).trim() + " left");
            return bits.join("   -   ");
        }
        // Large rounded banner: artwork with a dark caption panel and an action row.
        // options.fullscreen: the artwork fills the whole screen (screen backdrop) and the
        // text sits at the bottom over a full-width fade.
        function heroBlock(parent, it, options) {
            options = options || {};
            var width = Math.max(640, (viewport.clientWidth || 1280) - 96), height = Math.round(width * (options.tall ? 0.46 : 0.34)), img = null;
            if (options.fullscreen) height = Math.max(360, (viewport.clientHeight || 664) - 56);
            var box = el("div", parent, options.fullscreen
                ? {position: "relative", width: width + "px", height: height + "px"}
                : {position: "relative", width: width + "px", height: height + "px", borderRadius: "20px", overflow: "hidden", backgroundColor: C.panel, marginBottom: "18px"});
            if (!options.fullscreen) {
                img = el("img", box, {position: "absolute", left: "0", top: "0", width: width + "px", height: Math.round(width * 9 / 16) + "px", display: "none"});
                var shade = el("img", box, {position: "absolute", left: "0", top: "0", width: width + "px", height: height + "px"});
                shade.src = JF.scrimUrl;
            }
            var panel = el("div", box, {position: "absolute", left: "0", bottom: "0", maxWidth: Math.round(width * 0.64) + "px", padding: "26px 34px 30px"});
            var kicker = options.kicker ? el("div", panel, {fontSize: "17px", fontWeight: "bold", color: "rgba(255,255,255,0.7)", marginBottom: "6px"}, options.kicker) : null;
            var titleNode = el("div", panel, {fontSize: "44px", fontWeight: "bold", lineHeight: "1.08", marginBottom: "8px"}, it.Type === "Episode" && it.SeriesName && !options.episodeTitle ? it.SeriesName : it.Name);
            var metaNode = el("div", panel, {fontSize: "19px", color: "rgba(255,255,255,0.78)", marginBottom: "16px"}, metaLine(it));
            // Filled in with the full details, together with the action buttons.
            var overviewNode = options.overview ? el("div", panel, {fontSize: "20px", lineHeight: "1.4", maxHeight: "84px", overflow: "hidden", marginBottom: "20px", color: "rgba(255,255,255,0.9)"}, "") : null;
            var actions = el("div", panel, {display: "flex", alignItems: "center"});
            function setImage(item) {
                // An episode's page shows that episode's own still; everything else its backdrop.
                var pick = options.episodeStill && item.Type === "Episode" && item.ImageTags && item.ImageTags.Primary
                    ? {id: item.Id, kind: "Primary", tag: item.ImageTags.Primary} : heroImage(item);
                if (options.fullscreen) { if (pick) setBackdrop(pick, {dim: 0.15, fade: true}); }
                else if (pick) artwork.show(img, pick.id, pick.tag, 405, pick.kind);
            }
            setImage(it);
            return {box: box, img: img, kicker: kicker, titleNode: titleNode, metaNode: metaNode, overviewNode: overviewNode, actions: actions, setImage: setImage};
        }

        // ---- Screens ----------------------------------------------------------
        function connectScreen() {
            return {title: "Connect to a server", build: function (c) {
                el("div", c, {fontSize: "22px", color: C.dim, maxWidth: "1100px", lineHeight: "1.5", marginBottom: "16px"},
                    "Enter your Jellyfin address. At home: http://192.168.1.20:8096. Away from home: your HTTPS address, for example https://jellyfin.example.com (a path such as /jellyfin is fine).");
                function label() { return "Server: " + (settings.server || "not set"); }
                var warning = el("div", c, {fontSize: "19px", color: C.danger, maxWidth: "1100px", lineHeight: "1.4", margin: "4px 0 8px"});
                function warn() {
                    var text = "";
                    try {
                        var a = JF.jellyfin.parseServerAddress(settings.server);
                        if (JF.jellyfin.insecureRemote(a)) text = "Warning: this is an unencrypted (http://) address outside your home network. Your password and session can be read by anyone along the way. Use an https:// address for remote access.";
                    } catch (e) {}
                    warning.textContent = text;
                }
                var addr = ui.button(c, label(), async function () {
                    var value = await keyboard.open("Server address (up to 128 characters)", settings.server || "http://", {maxLength: 128, quickKeys: ["http://", "https://"]});
                    if (value === null) return;
                    // Keep exactly what was typed, so a long address never has to be retyped.
                    settings.server = value.trim(); persist();
                    addr.el.textContent = label(); warn();
                }, {wordBreak: "break-all", maxWidth: "1400px"});
                warn();
                function target() {
                    var a = JF.jellyfin.parseServerAddress(settings.server);
                    settings.host = a.host; settings.port = a.port; settings.tls = a.tls; settings.basePath = a.basePath; persist();
                    return a;
                }
                var go = ui.button(c, "Connect", function () {
                    var a;
                    try { a = target(); } catch (e) { status(e.message, true); return; }
                    guard("Connecting to " + a.host + "...", async function () {
                        makeApi();
                        var info = await api.publicInfo();
                        status("Connected to " + (info.ServerName || "Jellyfin") + " " + (info.Version || ""));
                        show(loginScreen(info), true);
                    });
                });
                var test = ui.button(c, "Test connection", function () {
                    var a;
                    try { a = target(); } catch (e) { status(e.message, true); return; }
                    if (!platform.testConnection) { status("Connection testing is only available on the console.", true); return; }
                    guard("Testing " + a.host + "...", async function () {
                        var result = await platform.testConnection(a, "/System/Info/Public");
                        result.lines.forEach(function (l) { log("Test: " + l); });
                        show(testResultScreen(result));
                    });
                });
                return [[addr], [go], [test], [ui.button(c, "Exit", close)]];
            }, back: function () { close(); }};
        }

        function testResultScreen(result) {
            return {title: result.ok ? "Connection test: OK" : "Connection test: failed", build: function (c) {
                result.lines.forEach(function (line) {
                    el("div", c, {fontSize: "21px", lineHeight: "1.5", color: /^FAILED/.test(line) ? C.danger : C.text, wordBreak: "break-word", maxWidth: "1500px"}, line);
                });
                el("div", c, {fontSize: "18px", color: C.dim, marginTop: "16px"}, "These lines are also in Settings > Diagnostics.");
                return [[ui.button(c, "Back", back)]];
            }};
        }

        function loginScreen(info) {
            var username = settings.userName || "", password = "";
            return {title: "Sign in" + (info && info.ServerName ? " to " + info.ServerName : ""), build: function (c) {
                var rows = [];
                var u = ui.button(c, "Username: " + (username || "-"), async function () {
                    var v = await keyboard.open("Username", username, {maxLength: 64}); if (v === null) return;
                    username = v; u.el.textContent = "Username: " + (username || "-");
                });
                var p = ui.button(c, "Password: " + (password ? "******" : "-"), async function () {
                    var v = await keyboard.open("Password", password, {secret: true, maxLength: 128}); if (v === null) return;
                    password = v; p.el.textContent = "Password: " + (password ? "******" : "-");
                });
                var go = ui.button(c, "Sign in", function () {
                    guard("Signing in...", async function () {
                        var who = await api.login(username, password);
                        password = "";
                        signedIn(who);
                    });
                }, {base: C.accentDark});
                rows.push([u], [p], [go]);
                var qc = ui.button(c, "Use Quick Connect (sign in from your phone)", function () {
                    guard("Starting Quick Connect...", async function () {
                        if (!(await api.quickConnectEnabled())) throw new Error("Quick Connect is turned off on this server (Dashboard > General).");
                        var start = await api.quickConnectStart();
                        show(quickConnectScreen(start));
                    });
                });
                rows.push([qc]);
                rows.push([ui.button(c, "Change server", function () { show(connectScreen(), true); })]);
                return rows;
            }, back: function () { show(connectScreen(), true); }};
        }

        function quickConnectScreen(start) {
            var stop = false;
            return {title: "Quick Connect", hint: "B: cancel", build: function (c) {
                el("div", c, {fontSize: "24px", color: C.dim, marginBottom: "16px"}, "On a signed-in device, open Jellyfin > your profile > Quick Connect and enter:");
                el("div", c, {fontSize: "96px", letterSpacing: "18px", color: C.accent, margin: "20px 0 30px", fontWeight: "bold"}, start.Code);
                var waiting = el("div", c, {fontSize: "22px", color: C.dim}, "Waiting for approval...");
                (async function poll() {
                    while (!stop && !closed) {
                        await platform.sleep(3000);
                        if (stop || closed) return;
                        try {
                            var state = await api.quickConnectState(start.Secret);
                            if (state && state.Authenticated) {
                                waiting.textContent = "Approved. Signing in...";
                                var who = await api.quickConnectFinish(start.Secret);
                                if (!stop) signedIn(who);
                                return;
                            }
                        } catch (e) { waiting.textContent = e.message; if (e.status === 404 || e.status === 401) { status("The Quick Connect code expired. Try again.", true); stop = true; back(); return; } }
                    }
                })();
                return [[ui.button(c, "Cancel", function () { stop = true; back(); })]];
            }, leave: function () { stop = true; }};
        }

        function signedIn(who) {
            settings.token = who.token; settings.userId = who.userId; settings.userName = who.userName || settings.userName;
            persist(); stack = []; show(homeScreen());
            status("Signed in as " + settings.userName);
        }

        function signOut() {
            settings.token = null; settings.userId = null; persist(); makeApi();
            stack = []; show(connectScreen()); show(loginScreen());
        }

        function homeScreen() {
            var screen = {isHome: true, tabKey: "home", title: "Home", hint: "L / R: tabs   A: select   B: exit", build: function (c) {
                var rows = [];
                var tabs = el("div", c, {display: "flex", alignItems: "center", margin: "0 0 20px"});
                var holder = el("div", c, {});
                var loading = el("div", holder, {fontSize: "22px", color: C.dim, marginTop: "20px"}, "Loading...");
                load("Loading your libraries...", async function () {
                    var results = await Promise.all([api.resume().catch(function () { return []; }), api.nextUp().catch(function () { return []; }), api.views(),
                        api.liveTvInfo().then(function (i) { return !!(i && i.IsEnabled); }).catch(function () { return false; })]);
                    liveTvEnabled = results[3];
                    if (current !== screen) return;
                    holder.removeChild(loading);
                    var views = results[2].filter(function (v) { return !v.CollectionType || ["movies", "tvshows", "homevideos", "mixed", "boxsets"].indexOf(v.CollectionType) >= 0; });
                    // Top tab bar, as on Apple TV.
                    tabViews = views;
                    rows.push(tabBar(tabs, "home"));
                    if (results[0].length) { heading(holder, "Continue watching"); addRows(rows, cardGrid(holder, results[0], openItem, 1, {landscape: true, carousel: true, gradient: true})); }
                    if (results[1].length) { heading(holder, "Next up"); addRows(rows, cardGrid(holder, results[1], openItem, 1, {landscape: true, carousel: true, gradient: true})); }
                    heading(holder, "Libraries");
                    if (!views.length) el("div", holder, {fontSize: "22px", color: C.dim}, "No video libraries are available to this account.");
                    else addRows(rows, cardGrid(holder, views, function (v) { goTab(v.Id); }, 0, {landscape: true}));
                    nav.setRows(rows, true);
                    if (screen.restoreTo) nav.focus(screen.restoreTo.row, screen.restoreTo.col);
            else if (screen.initialFocus) nav.focus(screen.initialFocus.row, screen.initialFocus.col);
                    else nav.focus(0, 0);                         // the Home tab, as on Apple TV
                    screen.restoreTo = null;
                    updateArtwork(nav.position().row);
                    homeReady();
                });
                return rows;
            }, back: function () { close(); }, options: function () { goTab("settings"); }};
            return screen;
        }

        function libraryScreen(view, options) {
            var items = [], total = 0, loadingMore = false, screen, tabRow = null, asTab = !!(options && options.tab);
            function loadPage(c, rows) {
                if (loadingMore) return;
                loadingMore = true;
                load("Loading " + view.Name + "...", async function () {
                    var first = !items.length;
                    var page = await api.library(view.Id, items.length, 60);
                    if (current !== screen) return;
                    total = page.total; items = sortAZ(items.concat(page.items));
                    var keep = screen.restoreTo || (first ? {row: tabRow ? 1 : 0, col: 0} : nav.position()); screen.restoreTo = null;
                    ui.clear(c); rows.length = 0;
                    if (tabRow) rows.push(tabRow);
                    el("div", c, {fontSize: "20px", color: C.dim, marginBottom: "6px"}, total + " titles - A-Z");
                    addRows(rows, cardGrid(c, items, openItem));
                    if (!items.length) el("div", c, {fontSize: "22px", color: C.dim}, "This library is empty.");
                    nav.setRows(rows, true); nav.focus(keep.row, keep.col); updateArtwork(nav.position().row);
                    // Fetch the next page when focus nears the end.
                    rows.forEach(function (row, r) { row.forEach(function (item) {
                        if (item === undefined || (tabRow && row === tabRow)) return;
                        var prev = item.onFocus; item.onFocus = function () { if (prev) prev(); if (r >= rows.length - 2 && items.length < total) loadPage(c, rows); };
                    }); });
                }).then(function () { loadingMore = false; });
            }
            screen = {title: view.Name, tabKey: asTab ? view.Id : null, hint: asTab ? "L / R: tabs   A: select   B: home" : "A: select   B: back", build: function (c) {
                var rows = [];
                if (asTab) {
                    var bar = el("div", c, {display: "flex", alignItems: "center", margin: "0 0 20px"});
                    tabRow = tabBar(bar, view.Id); rows.push(tabRow);
                }
                loadPage(el("div", c, {}), rows);
                return rows;
            }};
            return screen;
        }

        // A-Z by Jellyfin's sort name (which files "The Office" under O), numbers in
        // natural order, then year. Matches the server's own SortName ordering.
        function sortKey(it) { return String(it.SortName || it.Name || "").toLowerCase(); }
        function naturalCompare(a, b) {
            var ra = a.match(/\d+|\D+/g) || [], rb = b.match(/\d+|\D+/g) || [];
            for (var i = 0; i < Math.min(ra.length, rb.length); i++) {
                var x = ra[i], y = rb[i];
                if (x === y) continue;
                var nx = /^\d/.test(x), ny = /^\d/.test(y);
                if (nx && ny) { var d = Number(x) - Number(y); if (d) return d; return x.length - y.length; }
                return x < y ? -1 : 1;
            }
            return ra.length - rb.length;
        }
        function sortAZ(list) {
            return list.slice().sort(function (a, b) {
                return naturalCompare(sortKey(a), sortKey(b)) || ((a.ProductionYear || 0) - (b.ProductionYear || 0)) || (a.Id < b.Id ? -1 : a.Id > b.Id ? 1 : 0);
            });
        }

        function openItem(it) {
            if (it.Type === "Series") show(seriesScreen(it));
            else if (it.Type === "Season") show(seriesScreen({Id: it.SeriesId, Name: it.SeriesName}, it.Id));
            else if (it.Type === "BoxSet" || it.IsFolder) show(libraryScreen(it));
            else show(detailsScreen(it));
        }

        // Seasons in order (Season 1 first, Specials last); episodes by number.
        function bySeason(a, b) {
            var x = a.IndexNumber, y = b.IndexNumber;
            x = x === 0 ? 1e6 : (x == null ? 1e7 : x); y = y === 0 ? 1e6 : (y == null ? 1e7 : y);
            return x - y || String(a.Name).localeCompare(String(b.Name));
        }
        function byEpisode(a, b) {
            var x = a.IndexNumber == null ? 1e7 : a.IndexNumber, y = b.IndexNumber == null ? 1e7 : b.IndexNumber;
            return x - y || String(a.Name).localeCompare(String(b.Name));
        }

        // One episode in the right pane: thumbnail, number and title, runtime, overview.
        function episodeRow(parent, ep, width) {
            var rowHeight = 168;
            var row = el("div", parent, {position: "relative", display: "flex", alignItems: "flex-start", width: width + "px", height: rowHeight + "px", boxSizing: "border-box",
                padding: "12px", marginBottom: "12px", borderRadius: "16px", overflow: "hidden", outline: "4px solid transparent"});
            var shade = el("img", row, {position: "absolute", left: "0", top: "0", width: width + "px", height: rowHeight + "px"});
            shade.src = JF.gradientUrl;
            var thumb = el("div", row, {position: "relative", width: "256px", height: "144px", flexShrink: "0", borderRadius: "12px", backgroundColor: C.card, overflow: "hidden", marginRight: "22px"});
            var img = el("img", thumb, {position: "absolute", left: "0", top: "0", width: "256px", height: "144px", display: "none"});
            var progress = ep.UserData && ep.RunTimeTicks && ep.UserData.PlaybackPositionTicks ? ep.UserData.PlaybackPositionTicks / ep.RunTimeTicks : 0;
            if (progress > 0.01) {
                var track = el("div", thumb, {position: "absolute", left: "10px", right: "10px", bottom: "9px", height: "4px", borderRadius: "2px", backgroundColor: "rgba(255,255,255,0.3)", overflow: "hidden"});
                el("div", track, {height: "100%", width: Math.min(100, progress * 100) + "%", backgroundColor: C.text});
            }
            var text = el("div", row, {position: "relative", flex: "1", minWidth: "0"});
            var title = el("div", text, {fontSize: "22px", fontWeight: "bold", color: "#ffffff", whiteSpace: "nowrap", overflow: "hidden", textOverflow: "ellipsis"},
                (ep.IndexNumber != null ? ep.IndexNumber + ". " : "") + ep.Name);
            var meta = [];
            if (ep.RunTimeTicks) meta.push(Math.round(ep.RunTimeTicks / 600000000) + " min");
            if (ep.UserData && ep.UserData.Played) meta.push("Watched");
            el("div", text, {fontSize: "17px", color: "rgba(255,255,255,0.85)", margin: "4px 0 6px"}, meta.join("  -  "));
            el("div", text, {fontSize: "17px", lineHeight: "1.35", color: "rgba(255,255,255,0.9)", maxHeight: "46px", overflow: "hidden"}, ep.Overview || "");
            var pick = JF.jellyfin.landscapeImage(ep);
            return {el: row, rowEl: row, kind: "card", img: img, label: title, labelColors: {on: "#ffffff", off: "rgba(255,255,255,0.92)"}, activate: function () { show(detailsScreen(ep)); },
                imageId: pick && pick.id, imageTag: pick && pick.tag, imageKind: pick ? pick.kind : "Primary", imageHeight: 144};
        }

        function seriesScreen(series, seasonId) {
            var screen;
            screen = {title: series.Name, hint: "Left: seasons   Right: episodes   A: select   B: back", build: function (c) {
                var rows = [], seasons = [], seasonItems = [], episodeItems = [], selected = 0, right = null;
                // Width of an episode row: screen minus page padding, season pane, divider gap.
                var rowWidth = Math.max(420, (viewport.clientWidth || 1280) - 96 - 270 - 34 - 34 - 8);
                function position(item) {
                    for (var r = 0; r < rows.length; r++) { var col = rows[r].indexOf(item); if (col >= 0) return {row: r, col: col}; }
                    return null;
                }
                // Rows pair season i with episode i so both panes scroll together; the moves
                // below keep up/down inside a pane and make left/right switch panes.
                function layout(focusItem) {
                    rows.length = 0;
                    for (var i = 0; i < Math.max(seasonItems.length, episodeItems.length); i++) {
                        var row = [];
                        if (seasonItems[i]) row.push(seasonItems[i]);
                        if (episodeItems[i]) row.push(episodeItems[i]);
                        rows.push(row);
                    }
                    seasonItems.forEach(function (item, i) {
                        item.base = i === selected ? C.accentDark : C.pill;
                        item.moves = {
                            up: function () { return i > 0 ? position(seasonItems[i - 1]) : null; },
                            down: function () { return i < seasonItems.length - 1 ? position(seasonItems[i + 1]) : null; },
                            left: function () { return null; },
                            right: function () { return episodeItems.length ? position(episodeItems[0]) : null; }
                        };
                    });
                    episodeItems.forEach(function (item, i) {
                        item.moves = {
                            up: function () { return i > 0 ? position(episodeItems[i - 1]) : null; },
                            down: function () { return i < episodeItems.length - 1 ? position(episodeItems[i + 1]) : null; },
                            left: function () { return position(seasonItems[selected]); },
                            right: function () { return null; }
                        };
                    });
                    nav.setRows(rows, true);
                    var at = focusItem ? position(focusItem) : null;
                    if (at) nav.focus(at.row, at.col);
                    updateArtwork(nav.position().row);
                }
                function selectSeason(i, focusEpisodes) {
                    selected = i;
                    return load("Loading " + seasons[i].Name + "...", async function () {
                        var episodes = (await api.episodes(series.Id, seasons[i].Id)).slice().sort(byEpisode);
                        if (current !== screen) return;
                        ui.clear(right);
                        if (!episodes.length) el("div", right, {fontSize: "21px", color: C.dim, padding: "12px"}, "No episodes in this season.");
                        episodeItems = episodes.map(function (ep) { return episodeRow(right, ep, rowWidth); });
                        layout(focusEpisodes && episodeItems.length ? episodeItems[0] : seasonItems[i]);
                    });
                }
                load("Loading seasons...", async function () {
                    var loaded = await Promise.all([api.seasons(series.Id), api.item(series.Id).catch(function () { return null; })]);
                    seasons = loaded[0].slice().sort(bySeason);
                    if (current !== screen) return;
                    // The show's backdrop fills the screen behind both panes.
                    if (loaded[1]) setBackdrop(heroImage(loaded[1]));
                    if (!seasons.length) { el("div", c, {fontSize: "22px", color: C.dim}, "No seasons found."); return; }
                    var nameBox = el("div", c, {margin: "0 0 24px"});
                    el("div", nameBox, {display: "inline-block", fontSize: "44px", fontWeight: "bold", paddingBottom: "8px", borderBottom: "4px solid #ffffff"}, series.Name);
                    var panes = el("div", c, {display: "flex", alignItems: "stretch"});
                    var left = el("div", panes, {width: "270px", flexShrink: "0", marginRight: "34px"});
                    // A solid line between the seasons and the episodes.
                    right = el("div", panes, {flex: "1", minWidth: "0", borderLeft: "3px solid #ffffff", paddingLeft: "34px"});
                    seasonItems = seasons.map(function (s, i) {
                        var item = ui.button(left, s.Name, function () { selectSeason(i, true); },
                            {minWidth: "0", width: "260px", fontSize: "21px", padding: "12px 22px", margin: "0 0 10px"});
                        item.rowEl = item.el;
                        return item;
                    });
                    var wanted = seasons.findIndex(function (s) { return s.Id === seasonId; });
                    await selectSeason(wanted >= 0 ? wanted : 0, true);
                });
                return rows;
            }};
            return screen;
        }

        // ---- Live TV guide ----------------------------------------------------------
        var HALF_HOUR = 30 * 60000;
        // Times are shown in the chosen zone: "auto" uses the console's, otherwise a fixed
        // offset in minutes (the console's engine may not know the local zone).
        function zoneOffset() { return settings.tz === "auto" ? -new Date().getTimezoneOffset() : Number(settings.tz) || 0; }
        function zoneLabel(minutes) {
            var sign = minutes < 0 ? "-" : "+", a = Math.abs(minutes);
            return "UTC" + sign + (Math.floor(a / 60) < 10 ? "0" : "") + Math.floor(a / 60) + ":" + (a % 60 < 10 ? "0" : "") + (a % 60);
        }
        function clock(ms) {
            var d = new Date(ms + zoneOffset() * 60000), h = d.getUTCHours(), m = d.getUTCMinutes();
            return ((h % 12) || 12) + ":" + (m < 10 ? "0" : "") + m + (h < 12 ? " AM" : " PM");
        }
        function serverNow() { return api ? api.now() : Date.now(); }
        function byChannelNumber(a, b) {
            var x = String(a.Number || "").split(/[.\-]/).map(Number), y = String(b.Number || "").split(/[.\-]/).map(Number);
            for (var i = 0; i < Math.max(x.length, y.length); i++) {
                var p = isNaN(x[i]) ? 1e9 : (x[i] || 0), q = isNaN(y[i]) ? 1e9 : (y[i] || 0);
                if (p !== q) return p - q;
            }
            return String(a.Name).localeCompare(String(b.Name));
        }
        function programAt(list, time) {
            for (var i = 0; i < (list || []).length; i++) if (Date.parse(list[i].StartDate) <= time && time < Date.parse(list[i].EndDate)) return list[i];
            return null;
        }

        function guideScreen() {
            var screen, SPAN = 120, CH_W = 250, ROW_H = 84, ROWS = 30;
            var channels = [], programs = {}, tabRow = null, grid = null, ruler = null, info = null;
            var minStart = Math.floor(serverNow() / HALF_HOUR) * HALF_HOUR, maxStart = minStart + 10 * 60 * 60000;
            var windowStart = minStart, cursorTime = serverNow(), first = 0, focusChannel = 0, clockTimer = null;
            function width() { return Math.max(500, (viewport.clientWidth || 1280) - 96 - CH_W); }
            function showInfo(item) {
                if (!info) return;
                var p = item.program, c = item.channel;
                info.title.textContent = p ? p.Name : c.Name;
                info.meta.textContent = (c.Number ? c.Number + "  " : "") + c.Name + (p ? "   -   " + clock(Date.parse(p.StartDate)) + " - " + clock(Date.parse(p.EndDate)) +
                    (Date.parse(p.StartDate) <= serverNow() && serverNow() < Date.parse(p.EndDate) ? "   -   On now" : "") : "   -   No guide information");
                info.overview.textContent = p && p.Overview ? p.Overview : "";
            }
            function cellsFor(channel) {
                var list = programs[channel.Id] || [], windowEnd = windowStart + SPAN * 60000;
                return list.filter(function (p) { return Date.parse(p.EndDate) > windowStart && Date.parse(p.StartDate) < windowEnd; });
            }
            // Builds the visible slice of channel rows around `focusChannel`, focusing the
            // program on air at `cursorTime`.
            function render() {
                var rows = [tabRow], ppm = width() / SPAN, windowEnd = windowStart + SPAN * 60000, now = serverNow();
                first = Math.max(0, Math.min(focusChannel - 5, channels.length - ROWS));
                ui.clear(ruler); ui.clear(grid);
                for (var t = windowStart; t < windowEnd; t += HALF_HOUR) el("div", ruler, {position: "absolute", left: (CH_W + (t - windowStart) / 60000 * ppm) + "px", top: "0", fontSize: "17px", fontWeight: "bold", color: C.dim}, clock(t));
                if (now >= windowStart && now < windowEnd) el("div", ruler, {position: "absolute", left: (CH_W + (now - windowStart) / 60000 * ppm - 1) + "px", top: "26px", width: "3px", height: "10px", backgroundColor: C.text});
                var slice = channels.slice(first, first + ROWS);
                slice.forEach(function (channel, k) {
                    var index = first + k;
                    var rowEl = el("div", grid, {position: "relative", height: ROW_H + "px", marginBottom: "8px"});
                    // Channel cell on the Jellyfin blue-to-purple gradient.
                    var head = el("div", rowEl, {position: "absolute", left: "0", top: "0", width: (CH_W - 14) + "px", height: ROW_H + "px", borderRadius: "12px", backgroundColor: C.panel, overflow: "hidden"});
                    var headShade = el("img", head, {position: "absolute", left: "0", top: "0", width: (CH_W - 14) + "px", height: ROW_H + "px"});
                    headShade.src = JF.gradientUrl;
                    var logo = el("img", head, {position: "absolute", left: "12px", top: "18px", width: "80px", height: "48px", display: "none"});
                    el("div", head, {position: "absolute", left: "104px", right: "8px", top: "16px", fontSize: "20px", fontWeight: "bold", color: "#ffffff", whiteSpace: "nowrap", overflow: "hidden", textOverflow: "ellipsis"}, channel.Number || "");
                    el("div", head, {position: "absolute", left: "104px", right: "8px", top: "44px", fontSize: "16px", color: "rgba(255,255,255,0.9)", whiteSpace: "nowrap", overflow: "hidden", textOverflow: "ellipsis"}, channel.Name);
                    var list = cellsFor(channel), items = [];
                    (list.length ? list : [null]).forEach(function (p) {
                        var start = p ? Math.max(Date.parse(p.StartDate), windowStart) : windowStart, end = p ? Math.min(Date.parse(p.EndDate), windowEnd) : windowEnd;
                        var onNow = p ? Date.parse(p.StartDate) <= now && now < Date.parse(p.EndDate) : true;
                        var cell = el("div", rowEl, {position: "absolute", left: (CH_W + (start - windowStart) / 60000 * ppm) + "px", top: "0", width: Math.max(18, (end - start) / 60000 * ppm - 8) + "px",
                            height: ROW_H + "px", boxSizing: "border-box", padding: "12px 14px", borderRadius: "12px", overflow: "hidden", outline: "4px solid transparent",
                            backgroundColor: onNow ? "rgba(255,255,255,0.16)" : "rgba(255,255,255,0.07)"});
                        var label = el("div", cell, {fontSize: "19px", fontWeight: "bold", color: C.dim, whiteSpace: "nowrap", overflow: "hidden", textOverflow: "ellipsis"}, p ? p.Name : "No guide information");
                        if (p) el("div", cell, {fontSize: "15px", color: C.dim, marginTop: "4px", whiteSpace: "nowrap", overflow: "hidden", textOverflow: "ellipsis"}, clock(Date.parse(p.StartDate)) + " - " + clock(Date.parse(p.EndDate)));
                        if (p && onNow) {
                            var track = el("div", cell, {position: "absolute", left: "14px", right: "14px", bottom: "9px", height: "4px", borderRadius: "2px", backgroundColor: "rgba(255,255,255,0.25)", overflow: "hidden"});
                            el("div", track, {height: "100%", width: Math.min(100, (now - Date.parse(p.StartDate)) / (Date.parse(p.EndDate) - Date.parse(p.StartDate)) * 100) + "%", backgroundColor: C.text});
                        }
                        var item = {el: cell, rowEl: rowEl, kind: "card", label: label, program: p, channel: channel, channelIndex: index, start: p ? Date.parse(p.StartDate) : windowStart, end: p ? Date.parse(p.EndDate) : windowEnd,
                            onFocus: function () { showInfo(item); },
                            activate: function () {
                                if (!p || (Date.parse(p.StartDate) <= serverNow() && serverNow() < Date.parse(p.EndDate))) watchChannel(index);
                                else status(p.Name + " starts at " + clock(Date.parse(p.StartDate)) + ".");
                            }};
                        items.push(item);
                    });
                    items[0].img = logo; items[0].imageId = channel.ImageTags && channel.ImageTags.Primary ? channel.Id : null;
                    items[0].imageTag = channel.ImageTags && channel.ImageTags.Primary; items[0].imageKind = "Primary"; items[0].imageHeight = 60;
                    rows.push(items);
                });
                // Moves: up/down keep the time, left/right walk programs and slide the window.
                rows.forEach(function (row, r) {
                    if (r === 0) return;
                    row.forEach(function (item, col) {
                        item.moves = {
                            up: function () { return item.channelIndex === 0 ? {row: 0, col: Math.max(0, tabRow.length - 2)} : jump(item.channelIndex - 1); },
                            down: function () { return item.channelIndex < channels.length - 1 ? jump(item.channelIndex + 1) : null; },
                            left: function () {
                                if (col > 0) { cursorTime = Math.max(row[col - 1].start, windowStart); return {row: r, col: col - 1}; }
                                if (windowStart <= minStart) return null;
                                windowStart -= HALF_HOUR; cursorTime = Math.max(item.start - 60000, windowStart); focusChannel = item.channelIndex; render(); return null;
                            },
                            right: function () {
                                var windowEnd = windowStart + SPAN * 60000;
                                if (col < row.length - 1 && row[col + 1].start < windowEnd) { cursorTime = Math.max(row[col + 1].start, windowStart); return {row: r, col: col + 1}; }
                                if (windowStart >= maxStart) return null;
                                cursorTime = Math.max(item.end, windowEnd); windowStart = Math.min(maxStart, Math.floor(cursorTime / HALF_HOUR) * HALF_HOUR - HALF_HOUR * 2);
                                windowStart = Math.max(minStart, windowStart); focusChannel = item.channelIndex; render(); return null;
                            }
                        };
                    });
                });
                tabRow.forEach(function (t) { t.moves = {down: function () { return channels.length ? jump(first) : null; }}; });
                nav.setRows(rows, true);
                var target = locate(focusChannel);
                if (target) nav.focus(target.row, target.col);
                updateArtwork(nav.position().row);
                function jump(index) {
                    if (index < first || index >= first + ROWS) { focusChannel = index; render(); return null; }
                    focusChannel = index; return locate(index);
                }
                function locate(index) {
                    var r = index - first + 1;
                    if (r < 1 || r >= rows.length) return null;
                    var row = rows[r], best = 0;
                    for (var i = 0; i < row.length; i++) if (row[i].start <= cursorTime && cursorTime < row[i].end) { best = i; break; } else if (row[i].start <= cursorTime) best = i;
                    return {row: r, col: best};
                }
            }
            screen = {title: "Live TV", tabKey: "livetv", hint: "L / R: tabs   A: watch   B: home", leave: function () { if (clockTimer) { clearInterval(clockTimer); clockTimer = null; } }, build: function (c) {
                var rows = [];
                var bar = el("div", c, {display: "flex", alignItems: "center", margin: "0 0 18px"});
                tabRow = tabBar(bar, "livetv"); rows.push(tabRow);
                info = {box: el("div", c, {position: "relative", minHeight: "118px", marginBottom: "10px"})};
                // Live clock (server time, chosen zone) so the guide can be checked at a glance.
                var clockEl = el("div", info.box, {position: "absolute", right: "0", top: "0", textAlign: "right"});
                var clockTime = el("div", clockEl, {fontSize: "30px", fontWeight: "bold"}, clock(serverNow()));
                var clockZone = el("div", clockEl, {fontSize: "15px", color: C.dim}, "");
                function tickClock() {
                    clockTime.textContent = clock(serverNow());
                    clockZone.textContent = zoneLabel(zoneOffset()) + (settings.tz === "auto" ? " (console)" : "") + (api.clockSynced() ? " - server time" : "");
                }
                if (clockTimer) clearInterval(clockTimer);
                tickClock(); clockTimer = setInterval(function () { if (current !== screen) { clearInterval(clockTimer); clockTimer = null; return; } tickClock(); }, 1000);
                timers.add(clockTimer);
                info.title = el("div", info.box, {fontSize: "32px", fontWeight: "bold", whiteSpace: "nowrap", overflow: "hidden", textOverflow: "ellipsis", marginRight: "220px"}, "");
                info.meta = el("div", info.box, {fontSize: "18px", color: C.dim, margin: "6px 0 6px"}, "");
                info.overview = el("div", info.box, {fontSize: "18px", lineHeight: "1.35", color: "rgba(255,255,255,0.8)", maxHeight: "48px", overflow: "hidden"}, "");
                ruler = el("div", c, {position: "relative", height: "40px"});
                grid = el("div", c, {});
                load("Loading the guide...", async function () {
                    var all = [], total = Infinity;
                    while (all.length < total && all.length < 300) {
                        var page = await api.channels(all.length, 100);
                        total = page.total; all = all.concat(page.items);
                        if (!page.items.length) break;
                    }
                    if (current !== screen) return;
                    channels = all.sort(byChannelNumber);
                    if (!channels.length) { el("div", grid, {fontSize: "22px", color: C.dim}, "No Live TV channels are available."); return; }
                    var from = new Date(minStart), to = new Date(maxStart + SPAN * 60000);
                    for (var i = 0; i < channels.length; i += 40) {
                        var batch = channels.slice(i, i + 40), found = await api.programs(batch.map(function (ch) { return ch.Id; }), from, to);
                        found.forEach(function (p) { (programs[p.ChannelId] = programs[p.ChannelId] || []).push(p); });
                    }
                    if (current !== screen) return;
                    render();
                });
                return rows;
            }};
            // Tunes channel `index`; L / R during playback change channel.
            function watchChannel(index) {
                var channel = channels[index];
                if (!channel) return;
                var quality = QUALITIES[settings.quality] || QUALITIES[0], q = {videoBitrate: quality.videoBitrate, maxWidth: quality.maxWidth, maxHeight: quality.maxHeight};
                guard("Tuning " + (channel.Number ? channel.Number + " " : "") + channel.Name + "...", async function () {
                    var stream = await api.openLiveStream(channel.Id, q);
                    stream.path = api.livePlaylistPath(channel.Id, stream, q);
                    stream.channel = channel; stream.programs = programs[channel.Id] || [];
                    stream.switchChannel = function (delta) {
                        if (channels.length < 2) return;
                        var next = (index + delta + channels.length) % channels.length;
                        focusChannel = next; cursorTime = serverNow();
                        setTimeout(function () { watchChannel(next); }, 0);
                    };
                    play(channel, 0, stream);
                });
            }
            return screen;
        }

        function detailsScreen(it) {
            var screen;
            screen = {isDetails: true, title: it.Type === "Episode" && it.SeriesName ? it.SeriesName : it.Name, build: function (c) {
                var rows = [];
                var hero = heroBlock(c, it, {fullscreen: true, overview: true, episodeTitle: true, episodeStill: true, kicker: it.Type === "Episode" && it.SeriesName ? it.SeriesName.toUpperCase() : null});
                load("Loading details...", async function () {
                    var full = await api.item(it.Id);
                    if (current !== screen) return;
                    hero.titleNode.textContent = full.Name;
                    hero.metaNode.textContent = metaLine(full);
                    hero.overviewNode.textContent = full.Overview || "";
                    hero.setImage(full);
                    var resumeAt = full.UserData && full.UserData.PlaybackPositionTicks ? full.UserData.PlaybackPositionTicks / JF.jellyfin.TICKS : 0;
                    var buttons = [];
                    if (!full.MediaSources || !full.MediaSources.length) el("div", hero.actions, {fontSize: "20px", color: C.danger}, "This item has no playable media.");
                    else {
                        if (resumeAt > 30) buttons.push(ui.pill(hero.actions, "> Resume from " + ui.formatTime(resumeAt), function () { play(full, resumeAt); }));
                        buttons.push(ui.pill(hero.actions, resumeAt > 30 ? "Play from the beginning" : "> Play", function () { play(full, 0); }));
                    }
                    if (full.Type === "Episode" && full.SeriesId)
                        buttons.push(ui.pill(hero.actions, "All episodes", function () { show(seriesScreen({Id: full.SeriesId, Name: full.SeriesName}, full.SeasonId)); }));
                    if (buttons.length) rows.push(buttons);
                    nav.setRows(rows, true);
                });
                return rows;
            }};
            return screen;
        }

        function clockAt(zone) {
            var d = new Date(serverNow() + zone * 60000), h = d.getUTCHours(), m = d.getUTCMinutes();
            return ((h % 12) || 12) + ":" + (m < 10 ? "0" : "") + m + (h < 12 ? " AM" : " PM");
        }
        // A list of options; A picks one and returns to the previous screen.
        function choiceScreen(title, options, currentValue, onPick) {
            var at = 0;
            options.forEach(function (o, i) { if (String(o.value) === String(currentValue)) at = i; });
            return {title: title, initialFocus: {row: at, col: 0}, build: function (c) {
                return options.map(function (o) {
                    return [ui.button(c, (String(o.value) === String(currentValue) ? "(*) " : "( ) ") + o.label, function () { onPick(o.value); back(); }, {minWidth: "620px"})];
                });
            }};
        }

        // Rebuilds Settings after a change without losing the focused option.
        function refreshSettings() {
            var at = nav.position();
            show(settingsScreen(), true); nav.focus(at.row, at.col);
        }
        function settingsScreen() {
            return {title: "Settings", tabKey: "settings", hint: "L / R: tabs   A: select   B: home", initialFocus: {row: 1, col: 0}, build: function (c) {
                var rows = [];
                var bar = el("div", c, {display: "flex", alignItems: "center", margin: "0 0 10px"});
                rows.push(tabBar(bar, "settings"));
                heading(c, "Streaming quality");
                QUALITIES.forEach(function (q, i) {
                    var b = ui.button(c, (settings.quality === i ? "(*) " : "( ) ") + q.label, function () { settings.quality = i; persist(); refreshSettings(); }, {minWidth: "620px"});
                    rows.push([b]);
                });
                heading(c, "Playback");
                rows.push([ui.button(c, "Auto-play next episode: " + (settings.autoplay ? "On" : "Off"), function () {
                    settings.autoplay = !settings.autoplay; persist(); refreshSettings();
                }, {minWidth: "620px"})]);
                heading(c, "Connection");
                rows.push([ui.button(c, "Check HTTPS certificates: " + (settings.tlsVerify ? "On (recommended)" : "Off (insecure)"), function () {
                    settings.tlsVerify = !settings.tlsVerify; persist();
                    if (platform.setTlsVerify) platform.setTlsVerify(settings.tlsVerify);
                    refreshSettings();
                }, {minWidth: "620px"})]);
                heading(c, "Time");
                rows.push([ui.button(c, "Time zone: " + (settings.tz === "auto" ? "Auto (console: " + zoneLabel(-new Date().getTimezoneOffset()) + ")" : zoneLabel(Number(settings.tz))), function () {
                    var zones = ["auto"];
                    for (var h = -12; h <= 14; h++) { zones.push(h * 60); if (h === 3 || h === 4 || h === 5 || h === 9) zones.push(h * 60 + 30); if (h === 5) zones.push(5 * 60 + 45); }
                    show(choiceScreen("Time zone", zones.map(function (z) {
                        return {label: z === "auto" ? "Auto (console: " + zoneLabel(-new Date().getTimezoneOffset()) + ")" : zoneLabel(z) + "   (now " + clockAt(z) + ")", value: z};
                    }), settings.tz, function (z) { settings.tz = z; persist(); }));
                }, {minWidth: "620px"})]);
                heading(c, "Account");
                el("div", c, {fontSize: "20px", color: C.dim, marginBottom: "6px"}, (settings.userName || "Not signed in") + " on " + (settings.server || settings.host));
                rows.push([ui.button(c, "Sign out", signOut)]);
                rows.push([ui.button(c, "Diagnostics", function () { show(diagnosticsScreen()); })]);
                rows.push([ui.button(c, lastReport ? "Playback report" : "Playback report (none yet: press Y while playing)", function () { if (lastReport) show(reportScreen()); })]);
                rows.push([ui.button(c, "About & credits", function () { show(aboutScreen()); })]);
                return rows;
            }};
        }

        function aboutScreen() {
            var lines = [
                ["YAJA (Yet another jellyfin app) " + VERSION, "An unofficial client for Jellyfin media servers. Not affiliated with or endorsed by the Jellyfin project, Nintendo, Firaxis or 2K."],
                ["XITRIX - CivHomebrew", "The CivHomebrew launcher this app runs in: the .civapp format and packager, the app lifetime contract, and the way controller buttons are read natively."],
                ["XITRIX - Moonlight-CivSwitch", "The native video and audio pipeline used for playback (hardware H.264 decoding, the GPU texture, Opus and AudioOut), and the console patterns for network sockets, save files and the SDK lookup."],
                ["Gezine - CivSwitch", "The CivSwitch userland escape and its loader, whose primitives every native call here relies on."],
                ["The Jellyfin project", "The media server and its API."],
                ["Licensing", "This app's own code is MIT licensed. XITRIX's modules included in the package remain his work and are not covered by that license."]
            ];
            return {title: "About & credits", build: function (c) {
                var rows = [];
                lines.forEach(function (l) {
                    var block = el("div", c, {padding: "10px 14px", marginBottom: "6px", borderRadius: "12px", outline: "4px solid transparent", maxWidth: "1300px"});
                    el("div", block, {fontSize: "23px", fontWeight: "bold", marginBottom: "4px"}, l[0]);
                    el("div", block, {fontSize: "19px", lineHeight: "1.4", color: "rgba(255,255,255,0.8)"}, l[1]);
                    rows.push([{el: block, kind: "card"}]);
                });
                rows.push([ui.button(c, "Back", back)]);
                return rows;
            }};
        }

        function reportScreen() {
            return {title: "Playback report", build: function (c) {
                el("div", c, {fontSize: "18px", color: C.dim, marginBottom: "10px"}, lastReport.path
                    ? "Saved as " + lastReport.path + " (download it over CivHomebrew FTP: Root > " + lastReport.path + ")."
                    : "This report could not be saved to a file; take a photo of this screen.");
                var lines = lastReport.text.split("\n"), rows = [];
                for (var i = 0; i < lines.length; i += 8) {
                    var block = el("div", c, {fontSize: "16px", fontFamily: "monospace", whiteSpace: "pre", lineHeight: "1.35", padding: "4px 8px", borderRadius: "6px", outline: "4px solid transparent"});
                    block.textContent = lines.slice(i, i + 8).join("\n");
                    rows.push([{el: block, kind: "card"}]);
                }
                return rows;
            }};
        }

        function diagnosticsScreen() {
            return {title: "Diagnostics", build: function (c) {
                el("div", c, {fontSize: "18px", color: C.dim, marginBottom: "12px"}, "YAJA (Yet another jellyfin app) " + VERSION + " - device " + settings.deviceId);
                var box = el("div", c, {fontSize: "17px", fontFamily: "monospace", whiteSpace: "pre-wrap", lineHeight: "1.35"});
                box.textContent = diagnostics.slice().reverse().join("\n") || "No messages.";
                return [[ui.button(c, "Back", back)]];
            }};
        }

        // ---- Playback ---------------------------------------------------------
        var playback = null, lastReport = null;
        // liveStream (Live TV): {path, mediaSourceId, liveStreamId, playSessionId, channel, programs, switchChannel}
        function play(item, startSeconds, liveStream) {
            if (playback) return;
            var quality = QUALITIES[settings.quality] || QUALITIES[0];
            var stream, live = !!liveStream;
            if (live) stream = liveStream;
            else {
                try { stream = api.masterPlaylistPath(item, {videoBitrate: quality.videoBitrate, maxWidth: quality.maxWidth, maxHeight: quality.maxHeight, maxFramerate: 60}); }
                catch (e) { status(e.message, true); return; }
            }
            var screenEl, video, overlay, bar, fill, timeEl, stateEl, statsEl, hideTimer = null, started = false, lastProgressAt = 0, seekTarget = null, seekTimer = null;
            var sink;
            try {
                sink = platform.createSink({log: log, onImage: function (url) {
                    if (url.indexOf("live:/") === 0) video.style.backgroundImage = 'url("' + url + '")';
                    else video.style.backgroundImage = 'url("' + url + '")';
                }});
            } catch (e) { status("Video playback is unavailable: " + e.message, true); return; }

            screenEl = el("div", root, {position: "absolute", left: "0", top: "0", right: "0", bottom: "0", backgroundColor: "#000", zIndex: "20"});
            video = el("div", screenEl, {position: "absolute", left: "0", top: "0", right: "0", bottom: "0", backgroundSize: "contain", backgroundRepeat: "no-repeat", backgroundPosition: "center"});
            overlay = el("div", screenEl, {position: "absolute", left: "0", right: "0", bottom: "0", padding: "30px 64px 34px", backgroundColor: "rgba(0,0,0,0.55)"});
            if (live) el("div", overlay, {fontSize: "18px", fontWeight: "bold", color: "rgba(255,255,255,0.7)"}, "LIVE   -   " + ((item.Number ? item.Number + "  " : "") + item.Name).toUpperCase());
            else if (item.Type === "Episode" && item.SeriesName) el("div", overlay, {fontSize: "18px", fontWeight: "bold", color: "rgba(255,255,255,0.7)"}, item.SeriesName.toUpperCase() + "   -   S" + (item.ParentIndexNumber || "?") + ", E" + (item.IndexNumber || "?"));
            var nameEl = el("div", overlay, {fontSize: "32px", fontWeight: "bold", margin: "4px 0 18px"}, item.Name);
            bar = el("div", overlay, {height: "6px", backgroundColor: "rgba(255,255,255,0.28)", borderRadius: "3px", overflow: "hidden"});
            fill = el("div", bar, {height: "100%", width: "0%", backgroundColor: C.text});
            var line = el("div", overlay, {display: "flex", marginTop: "10px", fontSize: "19px", fontWeight: "bold"});
            timeEl = el("div", line, {flex: "1"}, "0:00");
            stateEl = el("div", line, {flex: "1", textAlign: "center", color: "rgba(255,255,255,0.8)"}, "Starting...");
            var remainEl = el("div", line, {flex: "1", textAlign: "right", color: "rgba(255,255,255,0.8)"}, "");
            // Media bar: focusable buttons while the overlay is showing.
            var controls = el("div", overlay, {display: "flex", alignItems: "center", marginTop: "16px"});
            el("div", overlay, {fontSize: "16px", color: "rgba(255,255,255,0.55)", marginTop: "12px"}, live
                ? "A: pause   Left/Right: buttons   L / R: channel   X: stats   Y: 30 s report   B: stop"
                : "A: pause   Left/Right: 10 s (buttons when shown)   L / R: 1 min   X: stats   Y: 30 s report   B: stop");
            statsEl = el("div", screenEl, {position: "absolute", left: "24px", top: "20px", fontSize: "16px", fontFamily: "monospace", whiteSpace: "pre", color: "#9f9", display: "none", backgroundColor: "rgba(0,0,0,0.6)", padding: "10px"});

            var perfNow = function () { return G.performance && G.performance.now ? G.performance.now() : Date.now(); };
            var upNext = null, nextEpisode = null;
            if (!live && item.Type === "Episode") api.nextEpisode(item).then(function (n) { nextEpisode = n; }).catch(function (e) { log("Next episode lookup: " + e.message); });
            var monitor = JF.diagnostics.create({now: perfNow, recordSeconds: JF.diagnostics.RECORD_SECONDS}), lastSecond = perfNow(), reportNote = "";
            var reportState = function () { return {itemId: item.Id, mediaSourceId: stream.mediaSourceId, playSessionId: stream.playSessionId, liveStreamId: stream.liveStreamId, position: player.position(), paused: player.paused()}; };
            function showOverlay() {
                overlay.style.display = "";
                if (hideTimer) clearTimeout(hideTimer);
                // Hide 5 s after the last input; if playback is buffering or paused then, keep
                // checking instead of giving up (otherwise a rebuffer left the bar on screen).
                hideTimer = setTimeout(function tryHide() {
                    if (menu || player.state() !== "playing") { hideTimer = setTimeout(tryHide, 1000); return; }
                    overlay.style.display = "none";
                }, 5000);
            }
            var player = JF.player.create({
                api: api, sink: sink, transport: platform.transport, sleep: platform.sleep, log: log,
                now: function () { return G.performance && G.performance.now ? G.performance.now() : Date.now(); },
                onState: function (s, detail) {
                    if (platform.keepAwake) platform.keepAwake(s === "loading" || s === "buffering" || s === "playing");
                    // Blank while playing (an empty label must not fall back to the raw state name).
                    var labels = {loading: live ? "Tuning..." : "Starting transcode...", buffering: "Buffering...", playing: live ? "LIVE" : "", paused: "Paused", ended: "Finished", error: "Error", stopped: ""};
                    stateEl.textContent = Object.prototype.hasOwnProperty.call(labels, s) ? labels[s] : s;
                    if (typeof playButton !== "undefined" && playButton) playButton.el.textContent = s === "paused" ? "> Play" : "|| Pause";
                    if (s === "playing" && !started) { started = true; api.reportStart(reportState()).catch(function (e) { log("Report start: " + e.message); }); }
                    if (s === "paused" || s === "buffering" || s === "error") showOverlay();
                    if (s === "error") status("Playback failed: " + (detail && detail.message), true);
                    if (s === "error") setTimeout(function () { stopPlayback(); }, 2500);
                    if (s === "ended") { if (settings.autoplay && nextEpisode) showUpNext(); else setTimeout(function () { stopPlayback(); }, 500); }
                }
            });
            function loop() {
                if (!playback || playback.player !== player) return;
                player.tick();
                var pos = player.position(), dur = player.duration();
                if (live) {
                    // Live: the bar shows how far into the current program we are.
                    var nowMs = serverNow(), prog = programAt(stream.programs, nowMs);
                    if (prog) {
                        var ps = Date.parse(prog.StartDate), pe = Date.parse(prog.EndDate);
                        nameEl.textContent = prog.Name;
                        fill.style.width = Math.min(100, (nowMs - ps) / (pe - ps) * 100) + "%";
                        timeEl.textContent = clock(ps); remainEl.textContent = clock(pe);
                    } else { nameEl.textContent = item.Name; fill.style.width = "100%"; timeEl.textContent = ""; remainEl.textContent = ""; }
                } else if (dur > 0) {
                    var at = seekTarget !== null ? seekTarget : pos;
                    fill.style.width = Math.min(100, at / dur * 100) + "%";
                    timeEl.textContent = ui.formatTime(at); remainEl.textContent = "-" + ui.formatTime(Math.max(0, dur - at));
                }
                if (started && Date.now() - lastProgressAt > 10000) { lastProgressAt = Date.now(); api.reportProgress(reportState()).catch(function (e) { log("Report progress: " + e.message); }); }
                monitor.tick(player.buffered());
                var tNow = perfNow();
                if (tNow - lastSecond >= 1000) { lastSecond = tNow; sampleSecond(); }
                schedule();
            }
            // Tick right before each render when the engine offers requestAnimationFrame,
            // with a timer as a backstop if frames are not being drawn.
            function schedule() {
                if (!playback || playback.player !== player) return;
                var fired = false;
                function run() { if (fired) return; fired = true; clearTimeout(playback.timer); if (playback && playback.frame && G.cancelAnimationFrame) G.cancelAnimationFrame(playback.frame); loop(); }
                if (typeof G.requestAnimationFrame === "function") playback.frame = G.requestAnimationFrame(run);
                playback.timer = setTimeout(run, 20);
            }
            function sampleSecond() {
                var net = platform.transport.stats || null, slice = net ? net.windowMaxMs || 0 : 0;
                if (net) net.windowMaxMs = 0;
                monitor.second({player: player.stats, sink: sink.stats ? sink.stats() : null, slice: slice, buffer: player.buffered(),
                    lead: player.stats.videoLeadMs, latency: sink.latency ? sink.latency() : null, playing: player.state() === "playing"});
                renderStats();
            }
            function renderStats() {
                if (statsEl.style.display === "none") return;
                var r = monitor.recent(), rec = monitor.recording(), lines = [];
                lines.push("LAST 5 S   " + player.state() + "   buffer " + player.buffered().toFixed(1) + " s   " + (sink.mode ? sink.mode() : "?") + " video");
                if (r) {
                    lines.push("shown " + r.shownFps.toFixed(1) + " fps   decoded " + r.decodedFps.toFixed(1) + "   fed " + r.fedFps.toFixed(1));
                    lines.push("frame spacing " + r.intervalMean.toFixed(1) + " +/- " + r.intervalJitter.toFixed(1) + " ms");
                    lines.push("loop pauses max " + r.maxGap + " ms, " + r.gaps50 + " over 50 ms   download slice max " + r.maxSlice + " ms");
                    lines.push("late " + r.late + "   skipped " + r.skipped + "   stalls " + r.stalls + "   replaced " + r.superseded + "   audio underruns " + r.underruns);
                    lines.push("decode " + r.decodeAvg.toFixed(1) + " ms   GPU copy " + r.gpuAvg.toFixed(1) + " ms   lead " + r.lead + " ms   latency " +
                        (r.latency !== null && r.latency !== undefined ? Math.round(r.latency * 1000) + " ms" : "?") + "   " + Math.round(r.kbps) + " kbps");
                } else lines.push("collecting...");
                if (rec) lines.push("RECORDING REPORT " + rec.seconds + " / " + rec.total + " s (keep playing)");
                if (reportNote) lines.push(reportNote);
                statsEl.textContent = lines.join("\n");
            }
            function startReport() {
                if (monitor.recording()) return;
                reportNote = "";
                monitor.record(function (rows) { buildReport(rows); });
                statsEl.style.display = ""; showOverlay(); renderStats();
                log("Recording a 30 s playback report");
            }
            async function buildReport(rows) {
                reportNote = "Building report..."; renderStats();
                var server = null;
                try { var session = await api.sessionInfo(); server = session && session.TranscodingInfo ? session.TranscodingInfo : null; }
                catch (e) { log("Session info: " + e.message); }
                var source = (item.MediaSources && item.MediaSources[0]) || {}, vs = null;
                (source.MediaStreams || []).forEach(function (m) { if (!vs && m.Type === "Video") vs = m; });
                var vi = player.videoInfo();
                var report = JF.diagnostics.formatReport(rows, {
                    version: VERSION, item: item.Type === "Episode" && item.SeriesName ? item.SeriesName + " - " + item.Name : item.Name,
                    sourceFps: vs ? (vs.RealFrameRate || vs.AverageFrameRate || null) : null, sourceRes: vs ? vs.Width + "x" + vs.Height : null,
                    sourceCodec: vs ? vs.Codec : null, stream: vi ? vi.width + "x" + vi.height + " H.264 (profile " + vi.profile + ", level " + vi.level + ")" : null,
                    quality: quality.label, mode: sink.mode ? sink.mode() : null, controller: controller ? "native" : "fallback",
                    server: server, startedAt: new Date().toISOString().replace("T", " ").slice(0, 19)
                });
                var path = null;
                try { if (platform.storage.writeText) path = platform.storage.writeText("reports/report-" + new Date().toISOString().replace(/[-:]/g, "").replace("T", "-").slice(0, 15) + ".txt", report.text); }
                catch (e) { log("Saving report failed: " + e.message); }
                lastReport = {text: report.text, path: path, findings: report.findings};
                log("Report: " + report.findings[0]);
                if (path) log("Report saved: " + path);
                reportNote = "REPORT READY: " + report.findings[0].split(":")[0] + (path ? "\nsaved " + path : "") + "\n(Settings > Playback report)";
                renderStats();
            }
            function showUpNext() {
                if (upNext || !playback || playback.player !== player) return;
                var seconds = JF.UP_NEXT_SECONDS || 10;
                overlay.style.display = "";
                var panel = el("div", screenEl, {position: "absolute", right: "64px", bottom: "200px", width: "560px", padding: "24px 28px",
                    backgroundColor: "rgba(28,28,30,0.94)", borderRadius: "20px", boxShadow: C.shadow});
                el("div", panel, {fontSize: "17px", fontWeight: "bold", color: "rgba(255,255,255,0.7)", marginBottom: "6px"}, "UP NEXT");
                el("div", panel, {fontSize: "26px", fontWeight: "bold", whiteSpace: "nowrap", overflow: "hidden", textOverflow: "ellipsis"},
                    "S" + (nextEpisode.ParentIndexNumber || "?") + ":E" + (nextEpisode.IndexNumber || "?") + " - " + nextEpisode.Name);
                var count = el("div", panel, {fontSize: "20px", color: C.dim, marginTop: "10px"});
                el("div", panel, {fontSize: "17px", color: C.dim, marginTop: "8px"}, "A: play now   B: back");
                upNext = {panel: panel, timer: null};
                function tickDown() {
                    count.textContent = "Starting in " + seconds + " s";
                    if (seconds-- <= 0) { playNext(); return; }
                    upNext.timer = setTimeout(tickDown, 1000);
                }
                tickDown();
            }
            function playNext() {
                if (!upNext) return;
                clearTimeout(upNext.timer); upNext = null;
                var next = nextEpisode;
                stopPlayback();
                status("Loading the next episode...");
                api.item(next.Id).then(function (full) {
                    if (closed || playback) return;
                    if (current && current.title !== undefined && stack.length && current.isDetails) show(detailsScreen(full), true);
                    status("");
                    play(full, 0);
                }).catch(function (e) { status("Next episode: " + e.message, true); });
            }
            function seekBy(delta) {
                if (live) { showOverlay(); return; }                       // no seeking in live TV
                seekTarget = Math.max(0, Math.min(player.duration() - 1, (seekTarget !== null ? seekTarget : player.position()) + delta));
                showOverlay(); stateEl.textContent = "Seek to " + ui.formatTime(seekTarget);
                // Coalesce repeated presses: each seek restarts the server's transcode.
                if (seekTimer) clearTimeout(seekTimer);
                seekTimer = setTimeout(function () { var t = seekTarget; seekTarget = null; seekTimer = null; player.seek(t); }, 700);
            }
            function stopPlayback() {
                if (platform.keepAwake) platform.keepAwake(false);
                if (!playback || playback.player !== player) return;
                var pb = playback; playback = null;
                clearTimeout(pb.timer); if (pb.frame && G.cancelAnimationFrame) G.cancelAnimationFrame(pb.frame);
                if (upNext) { clearTimeout(upNext.timer); upNext = null; }
                if (hideTimer) clearTimeout(hideTimer); if (seekTimer) clearTimeout(seekTimer);
                var state = reportState();
                monitor.cancel();
                player.stop(); try { sink.close(); } catch (e) { log("Sink close: " + e.message); }
                if (screenEl.parentNode) screenEl.parentNode.removeChild(screenEl);
                api.reportStopped(state).catch(function (e) { log("Report stopped: " + e.message); });
                api.stopTranscode(stream.playSessionId).catch(function () {});
                if (live && stream.liveStreamId) api.closeLiveStream(stream.liveStreamId).catch(function (e) { log("Close live stream: " + e.message); });
                if (current) render(current, true);
            }
            // ---- Media bar: pause, skip, subtitles, audio, quality -------------------------
            var source = (item.MediaSources && item.MediaSources[0]) || {}, streams = source.MediaStreams || [];
            var subtitleTracks = streams.filter(function (m) { return m.Type === "Subtitle"; });
            var audioTracks = streams.filter(function (m) { return m.Type === "Audio"; });
            var choice = {quality: settings.quality, audio: source.DefaultAudioStreamIndex != null ? source.DefaultAudioStreamIndex : (audioTracks[0] ? audioTracks[0].Index : null), subtitle: -1};
            function trackName(m) { return m.DisplayTitle || [m.Language, m.Title].filter(Boolean).join(" ") || "Track " + m.Index; }
            function qualityShort(i) { return (QUALITIES[i] || QUALITIES[0]).label.split(" (")[0]; }
            var buttons = [], barIndex = 0, menu = null;
            function addButton(label, run) { var b = ui.pill(controls, label, run, {fontSize: "19px", padding: "10px 24px"}); buttons.push(b); return b; }
            function paintBar() {
                buttons.forEach(function (b, i) {
                    var on = i === barIndex && !menu;
                    b.el.style.backgroundColor = on ? C.focus : C.pill; b.el.style.color = on ? "#000000" : C.text;
                    b.el.style.boxShadow = on ? "0 8px 22px rgba(0,0,0,0.5)" : "none";
                });
            }
            function togglePause() {
                player.toggle();
                playButton.el.textContent = player.paused() ? "> Play" : "|| Pause";
                api.reportProgress(reportState()).catch(function () {});
            }
            if (!live) addButton("<< 10", function () { seekBy(-10); });
            var playButton = addButton("|| Pause", togglePause);
            if (!live) addButton("10 >>", function () { seekBy(10); });
            var subtitleButton = !live && subtitleTracks.length ? addButton("Subtitles: Off", function () {
                openMenu("Subtitles", [{label: "Off", value: -1}].concat(subtitleTracks.map(function (m) { return {label: trackName(m), value: m.Index}; })), choice.subtitle, function (v) {
                    choice.subtitle = v; subtitleButton.el.textContent = "Subtitles: " + (v < 0 ? "Off" : trackName(subtitleTracks.filter(function (m) { return m.Index === v; })[0]));
                    restart();
                });
            }) : null;
            var audioButton = !live && audioTracks.length > 1 ? addButton("Audio: " + trackName(audioTracks.filter(function (m) { return m.Index === choice.audio; })[0] || audioTracks[0]), function () {
                openMenu("Audio", audioTracks.map(function (m) { return {label: trackName(m), value: m.Index}; }), choice.audio, function (v) {
                    choice.audio = v; audioButton.el.textContent = "Audio: " + trackName(audioTracks.filter(function (m) { return m.Index === v; })[0]);
                    restart();
                });
            }) : null;
            var qualityButton = addButton("Quality: " + qualityShort(choice.quality), function () {
                openMenu("Quality", QUALITIES.map(function (q, i) { return {label: q.label, value: i}; }), choice.quality, function (v) {
                    choice.quality = v; qualityButton.el.textContent = "Quality: " + qualityShort(v);
                    restart();
                });
            });
            barIndex = buttons.indexOf(playButton);
            paintBar();
            // Restarts the stream with the current choices, at the same moment (live: at the edge).
            function restart() {
                var q = QUALITIES[choice.quality] || QUALITIES[0], at = player.position(), old = stream.playSessionId;
                var opts = {videoBitrate: q.videoBitrate, maxWidth: q.maxWidth, maxHeight: q.maxHeight, maxFramerate: 60, audioStreamIndex: choice.audio, subtitleStreamIndex: choice.subtitle};
                api.stopTranscode(old).catch(function () {});
                if (live) { stream.path = api.livePlaylistPath(item.Id, stream, opts); player.load(stream.path, 0); }
                else { stream = api.masterPlaylistPath(item, opts); player.load(stream.path, at); }
                started = false; playButton.el.textContent = "|| Pause";
                log("Restarted the stream: quality " + qualityShort(choice.quality) + ", audio " + choice.audio + ", subtitles " + choice.subtitle);
            }
            // A small list over the video; up/down to choose, A to pick, B to close.
            function openMenu(title, options, currentValue, onPick) {
                var panel = el("div", screenEl, {position: "absolute", right: "64px", bottom: "230px", width: "520px", maxHeight: "60%", overflow: "hidden", padding: "18px 0",
                    backgroundColor: "rgba(28,28,30,0.96)", borderRadius: "20px", boxShadow: C.shadow, zIndex: "25"});
                el("div", panel, {fontSize: "18px", fontWeight: "bold", color: C.dim, padding: "0 24px 10px"}, title.toUpperCase());
                var list = el("div", panel, {});
                var at = 0, nodes = options.map(function (o, i) {
                    if (String(o.value) === String(currentValue)) at = i;
                    return el("div", list, {fontSize: "21px", padding: "11px 24px", whiteSpace: "nowrap", overflow: "hidden", textOverflow: "ellipsis"}, (String(o.value) === String(currentValue) ? "*  " : "   ") + o.label);
                });
                function paint() { nodes.forEach(function (n, i) { n.style.backgroundColor = i === at ? C.focus : "transparent"; n.style.color = i === at ? "#000000" : C.text; }); }
                function close() { if (panel.parentNode) panel.parentNode.removeChild(panel); menu = null; paintBar(); showOverlay(); }
                menu = {action: function (name) {
                    if (name === "up") at = Math.max(0, at - 1);
                    else if (name === "down") at = Math.min(options.length - 1, at + 1);
                    else if (name === "accept") { var v = options[at].value; close(); if (String(v) !== String(currentValue)) onPick(v); return; }
                    else if (name === "cancel") { close(); return; }
                    paint();
                }};
                paint(); paintBar(); showOverlay();
            }
            playback = {player: player, timer: null, stop: stopPlayback, action: function (name) {
                if (upNext) { if (name === "accept") playNext(); else if (name === "cancel") stopPlayback(); return; }
                if (menu) { menu.action(name); return; }
                var visible = overlay.style.display !== "none";
                if (name === "cancel") { stopPlayback(); return; }
                if (name === "menu") { statsEl.style.display = statsEl.style.display === "none" ? "" : "none"; renderStats(); return; }
                if (name === "options") { startReport(); return; }
                if ((name === "l" || name === "r") && live) { var sw = stream.switchChannel; stopPlayback(); if (sw) sw(name === "r" ? 1 : -1); return; }
                if (name === "l" || name === "r") { seekBy(name === "r" ? 60 : -60); return; }
                if (visible) {
                    // Bar showing: left/right choose a button, A presses it.
                    if (name === "left") barIndex = Math.max(0, barIndex - 1);
                    else if (name === "right") barIndex = Math.min(buttons.length - 1, barIndex + 1);
                    else if (name === "accept") { buttons[barIndex].activate(); }
                    paintBar(); showOverlay(); return;
                }
                // Bar hidden: A pauses, left/right skip 10 s, anything else shows the bar.
                if (name === "accept") { togglePause(); showOverlay(); }
                else if (name === "left" && !live) seekBy(-10);
                else if (name === "right" && !live) seekBy(10);
                else showOverlay();
            }};
            showOverlay();
            loop();
            player.load(stream.path, startSeconds || 0);
        }

        // ---- Input --------------------------------------------------------------
        var held = null, nextRepeat = 0, lastAction = {name: "", time: 0}, buttonFromEngine = function (name) { dispatch(name); };
        function dispatch(name) {
            if (closed) return;
            if (keyboard.active()) { keyboard.action(name); return; }
            if (playback) { playback.action(name); return; }
            if (busy && name !== "cancel") return;
            if (["up", "down", "left", "right"].indexOf(name) >= 0) nav.move(name);
            else if (name === "accept") nav.activate();
            else if (name === "cancel") { if (current && current.back) current.back(); else back(); }
            else if (name === "options" && current && (current.options || current.tabKey)) { if (current.options) current.options(); else goTab("settings"); }
            else if ((name === "l" || name === "r") && current && current.tabKey) cycleTab(name === "r" ? 1 : -1);
        }
        var NATIVE = {"nav-up": "up", "nav-down": "down", "nav-left": "left", "nav-right": "right", accept: "accept", cancel: "cancel", "keyboard-escape": "cancel", menu: "menu", options: "options"};
        function engineAction(name, phase, x, y) {
            phase = Number(phase);
            if (name === "nav-move") {
                x = Number(x); y = Number(y);
                if (Math.abs(x) > Math.abs(y) && Math.abs(x) > 0.5) name = x > 0 ? "nav-right" : "nav-left";
                else if (Math.abs(y) > 0.5) name = y > 0 ? "nav-up" : "nav-down";
                else { if (phase !== 0 && phase !== 3) held = null; return; }
            }
            var action = NATIVE[name];
            if (!action) return;
            if (phase !== 0 && phase !== 3) { if (held === action) held = null; return; }
            var t = Date.now();
            if (lastAction.name === action && t - lastAction.time < 90) return;
            if (["up", "down", "left", "right"].indexOf(action) >= 0) {
                if (phase === 3 && (held !== action || t < nextRepeat)) return;
                held = action; nextRepeat = t + (phase === 0 ? 350 : 120);
            } else if (phase !== 0) return;
            lastAction = {name: action, time: t};
            if (action === "menu" || action === "options") buttonFromEngine(action); else dispatch(action);
        }
        function listen(target, type, fn) { target.addEventListener(type, fn, true); removers.push(function () { target.removeEventListener(type, fn, true); }); }
        function consume(e) { if (e.preventDefault) e.preventDefault(); if (e.stopImmediatePropagation) e.stopImmediatePropagation(); }
        function onGame(e) {
            var d = e.detail || {};
            if (["nav-up", "nav-down", "nav-left", "nav-right", "nav-move", "accept", "cancel", "keyboard-escape", "menu", "options"].indexOf(d.name) < 0) return;
            engineAction(d.name, d.status, d.x, d.y); consume(e);
        }
        listen(window, "navigate-input", onGame);
        listen(window, "engine-input", onGame);
        listen(window, "keydown", function (e) {
            var map = {ArrowUp: "up", ArrowDown: "down", ArrowLeft: "left", ArrowRight: "right", Enter: "accept", Escape: "cancel", F8: "exit"};
            var name = map[e.key];
            // With the on-screen keyboard open, a physical keyboard types literally and
            // Enter finishes; the controller's + button arrives via gamepad polling.
            if (keyboard.active()) {
                if (e.key === "Enter") name = "plus";
                else if (e.key === "Backspace") name = "backspace";
                else if (e.key && e.key.length === 1) name = "char:" + e.key;
            }
            if (!name) name = {x: "menu", y: "options", q: "l", e: "r", "+": "plus"}[e.key];
            if (!name) return;
            consume(e);
            if (name === "exit") close(); else dispatch(name);
        });
        var nativeInput = false, subscription = null, owner = {owner: "jellyfin-civswitch"};
        if (G.engine && typeof G.engine.on === "function") {
            try { subscription = G.engine.on("InputAction", engineAction, owner); nativeInput = true; } catch (e) { log("Native input unavailable: " + e.message); }
        }
        // Buttons the engine does not report as input events (X, Y, L, R, ZR, +) are
        // read from the controller natively, the way CivHomebrew reads X. The
        // browser gamepad API is only a fallback where native reading is missing.
        var controller = null, lastButton = {}, previousBits = 0n, padState = {};
        if (platform.createController) {
            try { controller = platform.createController(); log("Controller buttons: native"); }
            catch (e) { log("Native controller buttons unavailable: " + e.message); }
        }
        var NPAD = [[0x4n, "menu"], [0x8n, "options"], [0x40n, "l"], [0x80n, "r"], [0x200n, "zr"], [0x400n, "plus"]];
        var PAD = nativeInput ? [[4, "l"], [5, "r"], [7, "zr"], [9, "plus"], [3, "menu"], [2, "options"]]
            : [[12, "up"], [13, "down"], [14, "left"], [15, "right"], [1, "accept"], [0, "cancel"], [3, "menu"], [2, "options"], [4, "l"], [5, "r"], [7, "zr"], [9, "plus"]];
        // One press may arrive by more than one route; act on it once.
        function button(name) {
            var t = Date.now();
            if (lastButton[name] && t - lastButton[name] < 150) return;
            lastButton[name] = t; dispatch(name);
        }
        buttonFromEngine = button;
        function pollPads() {
            if (closed) return;
            if (controller) {
                try {
                    var bits = controller.poll();
                    if (bits !== null) {
                        for (var n = 0; n < NPAD.length; n++) if ((bits & NPAD[n][0]) && !(previousBits & NPAD[n][0])) button(NPAD[n][1]);
                        previousBits = bits;
                    }
                } catch (e) { log("Controller polling stopped: " + e.message); try { controller.close(); } catch (_) {} controller = null; }
            } else {
                var pads = [];
                try { pads = navigator.getGamepads ? navigator.getGamepads() : []; } catch (e) { pads = []; }
                for (var p = 0; p < (pads ? pads.length : 0); p++) {
                    var pad = pads[p]; if (!pad) continue;
                    for (var i = 0; i < PAD.length; i++) {
                        var key = p + ":" + PAD[i][0], down = !!(pad.buttons[PAD[i][0]] && pad.buttons[PAD[i][0]].pressed);
                        if (down && !padState[key]) button(PAD[i][1]);
                        padState[key] = down;
                    }
                }
            }
            var t = setTimeout(pollPads, 33); timers.add(t);
        }
        pollPads();

        // ---- Lifetime -------------------------------------------------------------
        function close() {
            if (closed) return lifetime;
            closed = true;
            try { if (playback) playback.stop(); } catch (e) { log("Stop: " + e.message); }
            timers.forEach(clearTimeout); timers.clear();
            try { if (controller) controller.close(); } catch (e) {}
            removers.forEach(function (r) { r(); });
            try {
                if (subscription && typeof subscription.clear === "function") subscription.clear();
                else if (nativeInput && G.engine && typeof G.engine.off === "function") G.engine.off("InputAction", engineAction, owner);
            } catch (e) { log("Input cleanup: " + e.message); }
            if (root.parentNode) root.parentNode.removeChild(root);
            try { if (platform.close) platform.close(); } catch (e) {}
            if (G.__civswitchApp === instance) delete G.__civswitchApp;
            finish();
            return lifetime;
        }

        var instance = {id: "jellyfin-civswitch", name: "YAJA (Yet another jellyfin app)", close: close, lifetime: lifetime, diagnostics: diagnostics};
        G.__civswitchApp = instance;
        root.focus && root.focus();
        log("YAJA (Yet another jellyfin app) " + VERSION + " started");
        if (settings.host && settings.token && settings.userId) {
            splashNote.textContent = "Loading your libraries...";
            makeApi(); stack = [connectScreen()]; show(homeScreen());
        } else if (settings.host) {
            makeApi(); stack = [connectScreen()]; show(loginScreen()); hideSplash();
        } else { show(connectScreen()); hideSplash(); }
        return instance;
    }

    JF.startApp = startApp;
    JF.VERSION = VERSION;
})();

;
/* Entry point. Keep this file last in the bundle: its promise is the app lifetime. */
/* Credits: closing a previous app and returning the lifetime promise as the last
 * expression follow XITRIX's CivHomebrew Hello example; the CIVSWITCH_APP_READY
 * marker is that example's convention. */
(async function () {
    "use strict";
    var previous = window.__civswitchApp;
    if (previous && typeof previous.close === "function") await previous.close();
    var platform, startupError = null;
    try { platform = JF.nativePlatform.create(); }
    catch (error) {
        startupError = error;
        // Outside CivSwitch (for example a desktop browser preview) the UI still
        // runs; network and video calls report why they are unavailable.
        var memory = null;
        platform = {
            transport: {request: function () { return Promise.reject(new Error("Networking unavailable: " + startupError.message)); }},
            storage: {load: function () { return memory; }, save: function (data) { memory = JSON.parse(JSON.stringify(data)); }},
            createSink: function () { throw new Error(startupError.message); },
            sleep: function (ms) { return new Promise(function (r) { setTimeout(r, ms); }); }
        };
    }
    var app = JF.startApp(platform);
    if (startupError) JF.log("Platform unavailable: " + startupError.message);
    // Bounded startup marker for the PC-side loader log, as in the Hello example.
    if (typeof log === "function") { try { await log("CIVSWITCH_APP_READY"); } catch (e) {} }
    await app.lifetime;
})();

