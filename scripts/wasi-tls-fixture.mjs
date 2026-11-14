import {execFile} from "node:child_process";
import {mkdtemp,readFile,writeFile,rm} from "node:fs/promises";
import {tmpdir} from "node:os";
import {join} from "node:path";
import {promisify} from "node:util";
const openssl=args=>promisify(execFile)("openssl",args);

export async function wasiTlsFixture() {
  const directory=await mkdtemp(join(tmpdir(),"mcpx-wasi-trusted-tls-"));
  try {
    const ca=join(directory,"root.pem"),caKey=join(directory,"root.key"),otherCa=join(directory,"other.pem");
    for(const [name,cert,key] of [["mcpx root",ca,caKey],["mcpx other root",otherCa,join(directory,"other.key")]]) {
      await openssl(["req","-x509","-newkey","rsa:2048","-nodes","-keyout",key,"-out",cert,"-days","1","-subj",`/CN=${name}`,"-addext","basicConstraints=critical,CA:TRUE","-addext","keyUsage=critical,keyCertSign,cRLSign"]);
    }
    const certificates={};
    for(const [name,san,days] of [["good","IP:127.0.0.1,DNS:localhost","1"],["hostname","DNS:other.invalid","1"],["expired","IP:127.0.0.1,DNS:localhost","-1"]]) {
      const key=join(directory,`${name}.key`),csr=join(directory,`${name}.csr`),cert=join(directory,`${name}.pem`),extensions=join(directory,`${name}.cnf`);
      await openssl(["req","-new","-newkey","rsa:2048","-nodes","-keyout",key,"-out",csr,"-subj","/CN=localhost"]);
      await writeFile(extensions,`basicConstraints=critical,CA:FALSE\nkeyUsage=critical,digitalSignature,keyEncipherment\nextendedKeyUsage=serverAuth\nsubjectAltName=${san}\n`);
      if(name==="expired") {
        const database=join(directory,"index.txt"),serial=join(directory,"serial"),config=join(directory,"ca.cnf");
        await writeFile(database,"");await writeFile(serial,"01\n");
        await writeFile(config,`[ca]\ndefault_ca=issuer\n[issuer]\ndatabase=${database}\nserial=${serial}\nnew_certs_dir=${directory}\ncertificate=${ca}\nprivate_key=${caKey}\ndefault_md=sha256\npolicy=names\n[names]\ncommonName=supplied\n`);
        await openssl(["ca","-batch","-config",config,"-in",csr,"-out",cert,"-startdate","20200101000000Z","-enddate","20210101000000Z","-extfile",extensions]);
      } else {
        await openssl(["x509","-req","-in",csr,"-CA",ca,"-CAkey",caKey,"-CAcreateserial","-out",cert,"-days",days,"-extfile",extensions]);
      }
      certificates[name]={key:await readFile(key),cert:Buffer.concat([await readFile(cert),await readFile(ca)])};
    }
    return {ca,otherCa,...certificates,close:()=>rm(directory,{recursive:true,force:true})};
  } catch(error) {await rm(directory,{recursive:true,force:true});throw error;}
}
