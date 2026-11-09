"""v6.6.4 서버 거래소 (v6.7.0: 판매자·구매자 이름은 목록에 내보내지 않음 — 이름 보고 사 주는 몰아주기 방지. 관리자 통계에서만 확인) (붉은 보석 = 서버 지갑 보석)

휴대폰 지갑은 조작될 수 있으므로, 거래소의 돈은 서버 지갑(users.wallet.gems)만 씁니다.
서버 지갑 보석은 서버가 직접 계산한 보상(월드보스·결투장 우편)과 거래소 판매 대금으로만 늘어납니다.
- 구매: 한 트랜잭션에서 구매자 차감 · 판매자 입금(수수료 5% 제외) · 'sold' 처리 → 동시 구매·이중 결제 불가
- 장비 지급·반환: market_deliveries 에 기록 → 앱이 받아서 보관함에 넣고 확인(ack). 앱이 꺼져도 다시 받음
- 같은 요청 번호 재전송은 중복 처리하지 않음
- 이용 정지: Firestore bans/{uid} 가 있으면 거래 불가(사용자 토큰으로 조회, 5분 캐시)
한계: 장비 자체는 아직 휴대폰 저장이라, 판매 등록된 장비가 진짜인지는 서버가 확인할 수 없음
     → 관리자 통계(/api/market/admin)로 이상 거래를 찾아 정지
"""
import json, os, re, secrets, threading, time, urllib.request, urllib.error
from errors import Error

ITEMS={'flame':0,'laser':0,'ice':0,'thunder':0,'blood':1,'wind':1,'moonblade':1,'earth':1,'stormedge':2,'holy':2,'dragonfang':2,'void':2,'starfall':3,'nightcleaver':3,'demon':3,'frostpike':3,'gem-dawn':4,'gem-orbit':4,'sunaxe':4,'abyssblade':4,'phoenixsword':4,'skyline':4,'gem-eternity':5,'ember':0,'rune':0,'frost':0,'storm':0,'life':1,'moonward':1,'breeze':1,'steel':1,'stormguard':2,'paladin':2,'dragonwall':2,'shadow':2,'starshield':3,'nightguard':3,'hell':3,'frostguard':3,'gem-sprout':4,'sunguard':4,'abyssguard':4,'phoenixguard':4,'skyguard':4,'gem-cosmos':4,'gem-imperial':5}
MIN_PRICE=1
MAX_PRICE=1_000_000
# v6.6.7 가격 제한: 기준가의 0.5~3배만 등록 가능 (비싼 값으로 보석 몰아주기 · 헐값 몰아주기 방지)
RANK_BASE=[20,60,180,500,1500,5000]   # 일반·고급·레어·에픽·전설·신화 (+0 기준, 붉은 보석)
LOW,HIGH=0.5,3.0
def base_price(item,level):return max(1,round(RANK_BASE[ITEMS[item]]*(1+level*0.15)))
FEE=0.05
MAX_OPEN=10  # v6.9.5 동시 등록 5→10개
_cache={};_cache_lock=threading.Lock()

def firestore_doc_exists(collection,fb_uid,token,ttl=300):
    """사용자 본인 토큰으로 Firestore 문서 존재 여부 확인 (규칙상 로그인 사용자 읽기 가능한 bans/admins)"""
    pid=os.environ.get('FIREBASE_PROJECT_ID','').strip()
    if not pid or not token or not fb_uid:return False
    key=(collection,fb_uid)
    now=time.time()
    with _cache_lock:
        hit=_cache.get(key)
        if hit and hit[1]>now:return hit[0]
    url=f'https://firestore.googleapis.com/v1/projects/{pid}/databases/(default)/documents/{collection}/{fb_uid}'
    req=urllib.request.Request(url,headers={'Authorization':'Bearer '+token})
    try:
        with urllib.request.urlopen(req,timeout=4) as r:found=r.status==200
    except urllib.error.HTTPError as e:found=False if e.code in (403,404) else None
    except Exception:found=None
    if found is None:return False  # 확인 실패 시 막지 않음(정지 여부 확인용)
    with _cache_lock:_cache[key]=(found,now+ttl)
    return found

class Market:
    def __init__(self,service):
        self.s=service
        with service.tx() as c:
            c.executescript('''CREATE TABLE IF NOT EXISTS market(id TEXT PRIMARY KEY,seller TEXT,seller_name TEXT,item TEXT,level INTEGER,price INTEGER,status TEXT,buyer TEXT,buyer_name TEXT,created REAL,sold_at REAL,request_id TEXT,UNIQUE(seller,request_id));
CREATE INDEX IF NOT EXISTS market_status ON market(status);
CREATE INDEX IF NOT EXISTS market_seller ON market(seller);
CREATE TABLE IF NOT EXISTS market_exchanges(id TEXT PRIMARY KEY,uid TEXT,amount INTEGER,created REAL);
CREATE TABLE IF NOT EXISTS market_deliveries(id TEXT PRIMARY KEY,uid TEXT,item TEXT,level INTEGER,reason TEXT,created REAL,applied INTEGER DEFAULT 0);''')
    # ---------- 공통 ----------
    def fb_uid(self,uid):return uid[3:] if uid.startswith('fb_') else ''
    def check_ban(self,uid):
        if firestore_doc_exists('bans',self.fb_uid(uid),getattr(self.s.local,'token','')):raise Error('이용이 정지된 계정이라 거래소를 쓸 수 없습니다.',403)
    def wallet(self,c,uid):
        u=self.s.user(c,uid);w=json.loads(u['wallet']);w.setdefault('gems',0);return u,w
    def row(self,r,uid):
        d={'id':r['id'],'item':r['item'],'level':r['level'],'price':r['price'],'status':r['status'],'sellerId':self.fb_uid(r['seller']),'mine':r['seller']==uid,'created':r['created']}
        if r['seller']==uid:d.update(soldAt=r['sold_at'],proceeds=int(r['price']*(1-FEE)) if r['status']=='sold' else 0,requestId=r['request_id'])
        return d
    # ---------- 목록 ----------
    def ref_price(self,c,item,level):
        """기준가: 최근 14일 같은 장비·강화 판매가 3건 이상이면 중간값(기본 기준가의 0.5~2배 안으로), 아니면 기본 기준가"""
        base=base_price(item,level)
        rows=[r['price'] for r in c.execute("SELECT price FROM market WHERE item=? AND level=? AND status='sold' AND sold_at>? ORDER BY sold_at DESC LIMIT 15",(item,level,self.s.clock()-14*86400))]
        if len(rows)<3:return base
        rows.sort();mid=rows[len(rows)//2]
        return int(min(base*2,max(base*0.5,mid)))
    def price_range(self,c,item,level):
        ref=self.ref_price(c,item,level);return ref,max(MIN_PRICE,int(ref*LOW)),min(MAX_PRICE,int(ref*HIGH+0.999))
    def visible_ids(self,c):
        """v6.9.6 진열 규칙(몰아주기 방지): 같은 장비·강화 단계의 판매 중 물건 가격 중간값을 기준으로
        중간값 이하에서 가장 가까운 1개 + 중간값 초과에서 가장 가까운 1개만 진열(짝수 개면 가운데 두 값의 평균). 같은 가격이면 먼저 올린 물건. 나머지는 진열 대기(구매 불가)."""
        groups={}
        for r in c.execute("SELECT id,item,level,price,created FROM market WHERE status='open'"):
            if r['item'] in ITEMS:groups.setdefault((r['item'],r['level']),[]).append(r)
        shown=set()
        for rows in groups.values():
            ps=sorted(r['price'] for r in rows);n=len(ps);avg=ps[n//2] if n%2 else (ps[n//2-1]+ps[n//2])/2  # 중간값
            low=[r for r in rows if r['price']<=avg];high=[r for r in rows if r['price']>avg]
            if low:shown.add(min(low,key=lambda r:(avg-r['price'],r['created'],r['id']))['id'])
            if high:shown.add(min(high,key=lambda r:(r['price']-avg,r['created'],r['id']))['id'])
        return shown
    def overview(self,uid):
        with self.s.tx() as c:
            u,w=self.wallet(c,uid)
            shown=self.visible_ids(c)
            listings=[self.row(r,uid) for r in c.execute("SELECT * FROM market WHERE status='open' ORDER BY created DESC") if r['id'] in shown][:300]
            mine=[dict(self.row(r,uid),shown=r['id'] in shown) for r in c.execute('SELECT * FROM market WHERE seller=? AND (status=\'open\' OR created>?) ORDER BY created DESC LIMIT 50',(uid,self.s.clock()-7*86400))]
            n=c.execute('SELECT count(*) AS n FROM market_deliveries WHERE uid=? AND applied=0',(uid,)).fetchone()['n']
            refs={}
            for r in c.execute("SELECT DISTINCT item,level FROM market WHERE status='sold' AND sold_at>?",(self.s.clock()-14*86400,)):
                if r['item'] in ITEMS:refs[f"{r['item']}|{r['level']}"]=self.ref_price(c,r['item'],r['level'])
        return {'gems':int(w['gems']),'listings':listings,'mine':mine,'refs':refs,'rankBase':RANK_BASE,'priceLow':LOW,'priceHigh':HIGH,'pendingDeliveries':n,'fee':FEE,'maxOpen':MAX_OPEN,'display':'nearest-median','minPrice':MIN_PRICE,'maxPrice':MAX_PRICE}
    # ---------- 판매 등록 ----------
    def list_item(self,uid,b):
        request=self.s.request_id(b);item=str(b.get('item',''));level=b.get('level');price=b.get('price')
        if item not in ITEMS:raise Error('알 수 없는 장비입니다.')
        if type(level)!=int or not 0<=level<=30:raise Error('강화 단계가 올바르지 않습니다.')
        if type(price)!=int or not MIN_PRICE<=price<=MAX_PRICE:raise Error(f'가격은 붉은 보석 {MIN_PRICE}~{MAX_PRICE:,}로 정해 주세요.')
        self.check_ban(uid)
        with self.s.tx() as c:
            old=c.execute('SELECT * FROM market WHERE seller=? AND request_id=?',(uid,request)).fetchone()
            if old:return {'listing':self.row(old,uid),'already':True}
            ref,lo,hi=self.price_range(c,item,level)
            if not lo<=price<=hi:raise Error(f'이 장비는 붉은 보석 {lo:,}~{hi:,} 사이로만 올릴 수 있습니다. (기준가 {ref:,})')
            u=self.s.user(c,uid)
            if c.execute("SELECT count(*) AS n FROM market WHERE seller=? AND status='open'",(uid,)).fetchone()['n']>=MAX_OPEN:raise Error(f'동시에 {MAX_OPEN}개까지 등록할 수 있습니다.')
            lid='m'+secrets.token_hex(10)
            c.execute('INSERT INTO market VALUES(?,?,?,?,?,?,?,?,?,?,?,?)',(lid,uid,u['nickname'],item,level,price,'open','','',self.s.clock(),None,request))
            return {'listing':self.row(c.execute('SELECT * FROM market WHERE id=?',(lid,)).fetchone(),uid)}
    # ---------- 구매 ----------
    def buy(self,uid,b):
        lid=str(b.get('id',''));self.check_ban(uid)
        with self.s.tx() as c:
            r=c.execute('SELECT * FROM market WHERE id=?',(lid,)).fetchone()
            if not r:raise Error('없는 물건입니다.',404)
            if r['status']=='sold' and r['buyer']==uid:return {'already':True,'gems':int(self.wallet(c,uid)[1]['gems'])}
            if r['status']!='open':raise Error('이미 팔렸거나 취소된 물건입니다.',409)
            if r['seller']==uid:raise Error('내가 등록한 물건은 살 수 없습니다.')
            if lid not in self.visible_ids(c):raise Error('지금은 진열되지 않은 물건입니다. 목록을 새로고침해 주세요.',409)
            buyer,bw=self.wallet(c,uid)
            if bw['gems']<r['price']:raise Error(f"붉은 보석이 부족합니다. (필요 {r['price']:,} · 보유 {int(bw['gems']):,})")
            seller,sw=self.wallet(c,r['seller'])
            bw['gems']-=r['price'];sw['gems']=sw.get('gems',0)+int(r['price']*(1-FEE))
            c.execute('UPDATE users SET wallet=? WHERE id=?',(json.dumps(bw),uid));c.execute('UPDATE users SET wallet=? WHERE id=?',(json.dumps(sw),r['seller']))
            c.execute("UPDATE market SET status='sold',buyer=?,buyer_name=?,sold_at=? WHERE id=? AND status='open'",(uid,buyer['nickname'],self.s.clock(),lid))
            c.execute('INSERT OR IGNORE INTO market_deliveries(id,uid,item,level,reason,created) VALUES(?,?,?,?,?,?)',('buy:'+lid,uid,r['item'],r['level'],'buy',self.s.clock()))
            return {'gems':int(bw['gems']),'delivery':{'id':'buy:'+lid,'item':r['item'],'level':r['level']}}
    # ---------- 취소 ----------
    def cancel(self,uid,b):
        lid=str(b.get('id',''))
        with self.s.tx() as c:
            r=c.execute('SELECT * FROM market WHERE id=? AND seller=?',(lid,uid)).fetchone()
            if not r:raise Error('없는 물건입니다.',404)
            if r['status']=='cancelled':return {'already':True}
            if r['status']!='open':raise Error('이미 팔린 물건입니다.',409)
            c.execute("UPDATE market SET status='cancelled' WHERE id=?",(lid,))
            c.execute('INSERT OR IGNORE INTO market_deliveries(id,uid,item,level,reason,created) VALUES(?,?,?,?,?,?)',('cancel:'+lid,uid,r['item'],r['level'],'cancel',self.s.clock()))
            return {'delivery':{'id':'cancel:'+lid,'item':r['item'],'level':r['level']}}
    # ---------- 장비 받기 ----------
    def deliveries(self,uid):
        with self.s.tx() as c:return {'deliveries':[{'id':r['id'],'item':r['item'],'level':r['level'],'reason':r['reason']} for r in c.execute('SELECT * FROM market_deliveries WHERE uid=? AND applied=0 ORDER BY created',(uid,))]}
    def ack(self,uid,b):
        ids=b.get('ids')
        if not isinstance(ids,list) or len(ids)>100 or not all(isinstance(x,str) for x in ids):raise Error('요청 형식 오류')
        with self.s.tx() as c:
            for i in ids:c.execute('UPDATE market_deliveries SET applied=1 WHERE id=? AND uid=?',(i,uid))
        return {'ok':True}
    # ---------- 관리자 통계 ----------
    def admin(self,uid):
        if not firestore_doc_exists('admins',self.fb_uid(uid),getattr(self.s.local,'token',''),ttl=60):raise Error('관리자만 볼 수 있습니다.',403)
        since=self.s.clock()-7*86400;stats={}
        with self.s.tx() as c:
            for r in c.execute("SELECT * FROM market WHERE status='sold' AND sold_at>?",(since,)):
                for who,name,role,other in ((r['buyer'],r['buyer_name'],'buy',r['seller']),(r['seller'],r['seller_name'],'sell',r['buyer'])):
                    o=stats.setdefault(who,{'uid':who,'name':name,'buy':0,'sell':0,'n':0,'pairs':{}})
                    o[role]+=r['price'];o['n']+=1;o['pairs'][other]=o['pairs'].get(other,0)+1
            rows=[]
            for o in stats.values():
                o['repeat']=max(o['pairs'].values()) if o['pairs'] else 0;del o['pairs']
                u=c.execute('SELECT wallet FROM users WHERE id=?',(o['uid'],)).fetchone();o['gems']=json.loads(u['wallet']).get('gems',0) if u else 0
                o['firebaseUid']=self.fb_uid(o['uid']);rows.append(o)
        rows.sort(key=lambda o:-(o['buy']+o['sell']))
        return {'traders':rows[:100]}
    # ---------- v6.7.3 붉은 보석 → 파란 보석 교환 (1:1, 한쪽 방향만) ----------
    def exchange(self,uid,b):
        """서버 지갑의 붉은 보석을 빼고, 같은 수만큼 앱이 파란 보석(휴대폰 지갑)에 더함.
        같은 요청 번호 재전송은 한 번만 처리(already) → 앱은 교환 번호로 한 번만 더함"""
        request=self.s.request_id(b);amount=b.get('amount')
        if type(amount)!=int or not 1<=amount<=MAX_PRICE:raise Error(f'교환할 붉은 보석 수는 1~{MAX_PRICE:,}개로 정해 주세요.')
        self.check_ban(uid);eid='ex:'+uid+':'+request
        with self.s.tx() as c:
            old=c.execute('SELECT * FROM market_exchanges WHERE id=?',(eid,)).fetchone()
            if old:return {'already':True,'exchangeId':eid,'amount':int(old['amount']),'gems':int(self.wallet(c,uid)[1]['gems'])}
            u,w=self.wallet(c,uid)
            if w['gems']<amount:raise Error(f"붉은 보석이 부족합니다. (보유 {int(w['gems']):,})")
            w['gems']-=amount;c.execute('UPDATE users SET wallet=? WHERE id=?',(json.dumps(w),uid))
            c.execute('INSERT INTO market_exchanges VALUES(?,?,?,?)',(eid,uid,amount,self.s.clock()))
            return {'exchangeId':eid,'amount':amount,'gems':int(w['gems'])}
    def admin_grant(self,uid,b):
        """v6.7.5 관리자 모드: 관리자 계정(Firestore admins)에 붉은 보석 지급 (시험용, 한 번에 최대 10만)"""
        if not firestore_doc_exists('admins',self.fb_uid(uid),getattr(self.s.local,'token','')):raise Error('관리자만 사용할 수 있습니다.',403)
        amount=b.get('amount')
        if type(amount)!=int or not 1<=amount<=100000:raise Error('지급 수는 1~100,000개입니다.')
        with self.s.tx() as c:
            u,w=self.wallet(c,uid);w['gems']+=amount;c.execute('UPDATE users SET wallet=? WHERE id=?',(json.dumps(w),uid));return {'gems':int(w['gems'])}
    def dispatch(self,uid,method,path,b):
        if path=='/api/market' and method=='GET':return self.overview(uid)
        if path=='/api/market/list' and method=='POST':return self.list_item(uid,b)
        if path=='/api/market/buy' and method=='POST':return self.buy(uid,b)
        if path=='/api/market/cancel' and method=='POST':return self.cancel(uid,b)
        if path=='/api/market/deliveries' and method=='GET':return self.deliveries(uid)
        if path=='/api/market/ack' and method=='POST':return self.ack(uid,b)
        if path=='/api/market/admin' and method=='GET':return self.admin(uid)
        if path=='/api/market/exchange' and method=='POST':return self.exchange(uid,b)
        if path=='/api/market/admin-grant' and method=='POST':return self.admin_grant(uid,b)
        raise Error('없는 거래소 경로입니다.',404)
