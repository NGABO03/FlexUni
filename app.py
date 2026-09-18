import os, secrets, uuid, hmac, hashlib, json, threading, urllib.request, urllib.error
from datetime import datetime, timezone
from functools import wraps
from flask import Flask, render_template, request, redirect, url_for, session, flash, jsonify, abort
from flask_sqlalchemy import SQLAlchemy
from werkzeug.middleware.proxy_fix import ProxyFix
from sqlalchemy import or_, func, text
from werkzeug.security import generate_password_hash, check_password_hash
from werkzeug.utils import secure_filename
from flask_sock import Sock

try:
    import redis
except ImportError:
    redis = None
try:
    import boto3
except ImportError:
    boto3 = None

BASE = os.path.dirname(os.path.abspath(__file__))
UPLOAD = os.path.join(BASE, "static", "uploads")
os.makedirs(UPLOAD, exist_ok=True)

app = Flask(__name__)
app.config.update(
    SECRET_KEY=os.environ.get("SECRET_KEY") or secrets.token_hex(32),
    SQLALCHEMY_DATABASE_URI=(os.environ.get("DATABASE_URL", "sqlite:///flexuni.db").replace("postgres://", "postgresql+psycopg://", 1)),
    SQLALCHEMY_TRACK_MODIFICATIONS=False,
    MAX_CONTENT_LENGTH=int(os.environ.get("MAX_UPLOAD_BYTES", str(25 * 1024 * 1024))),
    SESSION_COOKIE_HTTPONLY=True,
    SESSION_COOKIE_SAMESITE="Lax",
    SESSION_COOKIE_SECURE=os.environ.get("COOKIE_SECURE", "0") == "1",
)
app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1)
db = SQLAlchemy(app)
sock = Sock(app)

# Realtime connection registry. Redis pub/sub is used when REDIS_URL is configured;
# otherwise the app falls back to a process-local registry for development.
_ws_lock = threading.RLock()
_ws_clients = {}
_redis_client = None
_redis_thread_started = False

def redis_client():
    global _redis_client
    if _redis_client is not None:
        return _redis_client
    if not redis or not os.environ.get("REDIS_URL"):
        return None
    try:
        _redis_client = redis.Redis.from_url(os.environ["REDIS_URL"], decode_responses=True)
        _redis_client.ping()
        return _redis_client
    except Exception:
        _redis_client = None
        return None

def _redis_listener():
    r = redis_client()
    if not r:
        return
    try:
        pubsub = r.pubsub(ignore_subscribe_messages=True)
        pubsub.subscribe(os.environ.get("REDIS_WS_CHANNEL", "flexuni:ws"))
        for message in pubsub.listen():
            try:
                event = json.loads(message["data"])
                _ws_publish_local(int(event["user_id"]), event["payload"])
            except Exception:
                continue
    except Exception:
        return

def ensure_redis_listener():
    global _redis_thread_started
    if _redis_thread_started or not redis_client():
        return
    _redis_thread_started = True
    threading.Thread(target=_redis_listener, daemon=True, name="flexuni-redis-ws").start()

def _ws_publish_local(user_id, event):
    payload=json.dumps(event, separators=(",", ":"))
    dead=[]
    with _ws_lock:
        for ws in list(_ws_clients.get(user_id, set())):
            try:
                ws.send(payload)
            except Exception:
                dead.append(ws)
        for ws in dead:
            ws_unregister(user_id, ws)

def ws_register(user_id, ws):
    with _ws_lock:
        _ws_clients.setdefault(user_id, set()).add(ws)

def ws_unregister(user_id, ws):
    with _ws_lock:
        clients = _ws_clients.get(user_id, set())
        clients.discard(ws)
        if not clients:
            _ws_clients.pop(user_id, None)

def ws_publish(user_id, event):
    r = redis_client()
    if r:
        ensure_redis_listener()
        try:
            r.publish(os.environ.get("REDIS_WS_CHANNEL", "flexuni:ws"), json.dumps({"user_id": int(user_id), "payload": event}, separators=(",", ":")))
            return
        except Exception:
            pass
    _ws_publish_local(user_id, event)

def ws_publish_many(user_ids, event):
    for user_id in set(user_ids):
        ws_publish(user_id, event)

ALLOWED_IMAGES={"jpg","jpeg","png","webp"}
ALLOWED_MEDIA=ALLOWED_IMAGES | {"mp4","webm","mov"}

class User(db.Model):
    id=db.Column(db.Integer,primary_key=True); name=db.Column(db.String(120),nullable=False)
    email=db.Column(db.String(255),unique=True,nullable=False,index=True); password=db.Column(db.String(255),nullable=False)
    university=db.Column(db.String(180),default="University of Rwanda"); course=db.Column(db.String(180),default="")
    bio=db.Column(db.Text,default=""); photo=db.Column(db.String(255),default=""); role=db.Column(db.String(30),default="student")
    created_at=db.Column(db.DateTime,default=lambda:datetime.now(timezone.utc))

class Post(db.Model):
    id=db.Column(db.Integer,primary_key=True); user_id=db.Column(db.Integer,db.ForeignKey("user.id"),nullable=False,index=True)
    body=db.Column(db.Text,nullable=False); media=db.Column(db.String(255),default=""); media_type=db.Column(db.String(30),default="")
    created_at=db.Column(db.DateTime,default=lambda:datetime.now(timezone.utc),index=True)
    user=db.relationship("User")
    likes=db.relationship("Like", backref="post", cascade="all, delete-orphan")
    comments=db.relationship("Comment", backref="post", cascade="all, delete-orphan", order_by="Comment.created_at.asc()")
    shares=db.relationship("Share", backref="post", cascade="all, delete-orphan")

class Like(db.Model):
    user_id=db.Column(db.Integer,db.ForeignKey("user.id"),primary_key=True); post_id=db.Column(db.Integer,db.ForeignKey("post.id"),primary_key=True)
class Share(db.Model):
    id=db.Column(db.Integer,primary_key=True); user_id=db.Column(db.Integer,db.ForeignKey("user.id"),nullable=False); post_id=db.Column(db.Integer,db.ForeignKey("post.id"),nullable=False); created_at=db.Column(db.DateTime,default=lambda:datetime.now(timezone.utc))
    __table_args__=(db.UniqueConstraint("user_id","post_id",name="uq_share_user_post"),)
class Comment(db.Model):
    id=db.Column(db.Integer,primary_key=True); user_id=db.Column(db.Integer,db.ForeignKey("user.id"),nullable=False); post_id=db.Column(db.Integer,db.ForeignKey("post.id"),nullable=False)
    body=db.Column(db.Text,nullable=False); created_at=db.Column(db.DateTime,default=lambda:datetime.now(timezone.utc)); user=db.relationship("User")
class Community(db.Model):
    id=db.Column(db.Integer,primary_key=True); name=db.Column(db.String(120),unique=True,nullable=False); description=db.Column(db.Text,default="")
class Membership(db.Model):
    user_id=db.Column(db.Integer,db.ForeignKey("user.id"),primary_key=True); community_id=db.Column(db.Integer,db.ForeignKey("community.id"),primary_key=True)
class Product(db.Model):
    id=db.Column(db.Integer,primary_key=True); seller_id=db.Column(db.Integer,db.ForeignKey("user.id"),nullable=False); title=db.Column(db.String(180),nullable=False)
    price=db.Column(db.Numeric(12,2),nullable=False); category=db.Column(db.String(100),default=""); description=db.Column(db.Text,default=""); image=db.Column(db.String(255),default="")
    created_at=db.Column(db.DateTime,default=lambda:datetime.now(timezone.utc)); seller=db.relationship("User")
class Message(db.Model):
    id=db.Column(db.Integer,primary_key=True); sender_id=db.Column(db.Integer,db.ForeignKey("user.id"),nullable=False); receiver_id=db.Column(db.Integer,db.ForeignKey("user.id"),nullable=False)
    body=db.Column(db.Text,nullable=False); read=db.Column(db.Boolean,default=False); created_at=db.Column(db.DateTime,default=lambda:datetime.now(timezone.utc),index=True)
class Notification(db.Model):
    id=db.Column(db.Integer,primary_key=True); user_id=db.Column(db.Integer,db.ForeignKey("user.id"),nullable=False,index=True); kind=db.Column(db.String(50)); text=db.Column(db.String(255)); url=db.Column(db.String(255),default=""); read=db.Column(db.Boolean,default=False); created_at=db.Column(db.DateTime,default=lambda:datetime.now(timezone.utc))
class Resource(db.Model):
    id=db.Column(db.Integer,primary_key=True); user_id=db.Column(db.Integer,db.ForeignKey("user.id"),nullable=False); title=db.Column(db.String(180),nullable=False); description=db.Column(db.Text,default=""); link=db.Column(db.String(500),default=""); category=db.Column(db.String(80),default="Study"); created_at=db.Column(db.DateTime,default=lambda:datetime.now(timezone.utc)); owner=db.relationship("User")
class Opportunity(db.Model):
    id=db.Column(db.Integer,primary_key=True); user_id=db.Column(db.Integer,db.ForeignKey("user.id"),nullable=False); title=db.Column(db.String(180),nullable=False); description=db.Column(db.Text,default=""); link=db.Column(db.String(500),default=""); category=db.Column(db.String(80),default="Opportunity"); deadline=db.Column(db.DateTime); created_at=db.Column(db.DateTime,default=lambda:datetime.now(timezone.utc))
    owner=db.relationship("User")
class ResourceBookmark(db.Model):
    user_id=db.Column(db.Integer,db.ForeignKey("user.id"),primary_key=True)
    resource_id=db.Column(db.Integer,db.ForeignKey("resource.id"),primary_key=True)
class Report(db.Model):
    id=db.Column(db.Integer,primary_key=True)
    reporter_id=db.Column(db.Integer,db.ForeignKey("user.id"),nullable=False,index=True)
    target_type=db.Column(db.String(40),nullable=False)
    target_id=db.Column(db.Integer,nullable=False,index=True)
    reason=db.Column(db.String(120),nullable=False)
    details=db.Column(db.Text,default="")
    status=db.Column(db.String(30),default="open",index=True)
    reviewed_by=db.Column(db.Integer,db.ForeignKey("user.id"))
    created_at=db.Column(db.DateTime,default=lambda:datetime.now(timezone.utc),index=True)
    reviewed_at=db.Column(db.DateTime)
    reporter=db.relationship("User",foreign_keys=[reporter_id])

class ModerationAction(db.Model):
    id=db.Column(db.Integer,primary_key=True)
    admin_id=db.Column(db.Integer,db.ForeignKey("user.id"),nullable=False)
    action=db.Column(db.String(60),nullable=False)
    target_type=db.Column(db.String(40),nullable=False)
    target_id=db.Column(db.Integer,nullable=False)
    note=db.Column(db.Text,default="")
    created_at=db.Column(db.DateTime,default=lambda:datetime.now(timezone.utc),index=True)
    admin=db.relationship("User")

class UserModeration(db.Model):
    user_id=db.Column(db.Integer,db.ForeignKey("user.id"),primary_key=True)
    status=db.Column(db.String(30),default="active")
    reason=db.Column(db.Text,default="")
    expires_at=db.Column(db.DateTime)
    updated_at=db.Column(db.DateTime,default=lambda:datetime.now(timezone.utc),onupdate=lambda:datetime.now(timezone.utc))
    user=db.relationship("User")

class BusinessAccount(db.Model):
    id=db.Column(db.Integer,primary_key=True)
    owner_id=db.Column(db.Integer,db.ForeignKey("user.id"),nullable=False,unique=True)
    name=db.Column(db.String(180),nullable=False)
    description=db.Column(db.Text,default="")
    website=db.Column(db.String(500),default="")
    verified=db.Column(db.Boolean,default=False)
    created_at=db.Column(db.DateTime,default=lambda:datetime.now(timezone.utc))
    owner=db.relationship("User")

class University(db.Model):
    id=db.Column(db.Integer,primary_key=True)
    name=db.Column(db.String(180),nullable=False,unique=True)
    city=db.Column(db.String(120),default="")
    country=db.Column(db.String(120),default="Rwanda")
    description=db.Column(db.Text,default="")
    website=db.Column(db.String(500),default="")
    verified=db.Column(db.Boolean,default=False)
    created_at=db.Column(db.DateTime,default=lambda:datetime.now(timezone.utc))

class UniversityMembership(db.Model):
    id=db.Column(db.Integer,primary_key=True)
    user_id=db.Column(db.Integer,db.ForeignKey("user.id"),nullable=False,index=True)
    university_id=db.Column(db.Integer,db.ForeignKey("university.id"),nullable=False,index=True)
    role=db.Column(db.String(30),default="student")
    status=db.Column(db.String(30),default="active",index=True)
    joined_at=db.Column(db.DateTime,default=lambda:datetime.now(timezone.utc))
    __table_args__=(db.UniqueConstraint("user_id","university_id",name="uq_university_membership"),)
    user=db.relationship("User",backref="university_memberships")
    university=db.relationship("University",backref="memberships")

class UniversityJoinRequest(db.Model):
    id=db.Column(db.Integer,primary_key=True)
    user_id=db.Column(db.Integer,db.ForeignKey("user.id"),nullable=False,index=True)
    university_id=db.Column(db.Integer,db.ForeignKey("university.id"),nullable=False,index=True)
    message=db.Column(db.Text,default="")
    status=db.Column(db.String(30),default="pending",index=True)
    created_at=db.Column(db.DateTime,default=lambda:datetime.now(timezone.utc),index=True)
    reviewed_at=db.Column(db.DateTime)
    user=db.relationship("User")
    university=db.relationship("University")

class AIConversation(db.Model):
    id=db.Column(db.Integer,primary_key=True)
    user_id=db.Column(db.Integer,db.ForeignKey("user.id"),nullable=False,index=True)
    title=db.Column(db.String(180),default="Study Assistant")
    created_at=db.Column(db.DateTime,default=lambda:datetime.now(timezone.utc))
    user=db.relationship("User")

class AIMessage(db.Model):
    id=db.Column(db.Integer,primary_key=True)
    conversation_id=db.Column(db.Integer,db.ForeignKey("ai_conversation.id"),nullable=False,index=True)
    role=db.Column(db.String(20),nullable=False)
    body=db.Column(db.Text,nullable=False)
    created_at=db.Column(db.DateTime,default=lambda:datetime.now(timezone.utc),index=True)

class VideoRoom(db.Model):
    id=db.Column(db.String(36),primary_key=True)
    host_id=db.Column(db.Integer,db.ForeignKey("user.id"),nullable=False,index=True)
    title=db.Column(db.String(180),nullable=False)
    university_id=db.Column(db.Integer,db.ForeignKey("university.id"),index=True)
    active=db.Column(db.Boolean,default=True,index=True)
    created_at=db.Column(db.DateTime,default=lambda:datetime.now(timezone.utc))
    host=db.relationship("User")

class LiveStream(db.Model):
    id=db.Column(db.String(36),primary_key=True)
    host_id=db.Column(db.Integer,db.ForeignKey("user.id"),nullable=False,index=True)
    title=db.Column(db.String(180),nullable=False)
    description=db.Column(db.Text,default="")
    university_id=db.Column(db.Integer,db.ForeignKey("university.id"),index=True)
    active=db.Column(db.Boolean,default=True,index=True)
    created_at=db.Column(db.DateTime,default=lambda:datetime.now(timezone.utc))
    host=db.relationship("User")

class LiveMessage(db.Model):
    id=db.Column(db.Integer,primary_key=True)
    stream_id=db.Column(db.String(36),db.ForeignKey("live_stream.id"),nullable=False,index=True)
    user_id=db.Column(db.Integer,db.ForeignKey("user.id"),nullable=False)
    body=db.Column(db.String(500),nullable=False)
    created_at=db.Column(db.DateTime,default=lambda:datetime.now(timezone.utc),index=True)

class CampusService(db.Model):
    id=db.Column(db.Integer,primary_key=True)
    business_id=db.Column(db.Integer,db.ForeignKey("business_account.id"),nullable=False,index=True)
    university_id=db.Column(db.Integer,db.ForeignKey("university.id"),index=True)
    name=db.Column(db.String(180),nullable=False)
    category=db.Column(db.String(100),default="General")
    description=db.Column(db.Text,default="")
    price_label=db.Column(db.String(100),default="")
    location=db.Column(db.String(240),default="")
    phone=db.Column(db.String(50),default="")
    contact_url=db.Column(db.String(500),default="")
    active=db.Column(db.Boolean,default=True,index=True)
    created_at=db.Column(db.DateTime,default=lambda:datetime.now(timezone.utc))
    business=db.relationship("BusinessAccount")
    university=db.relationship("University")

class Subscription(db.Model):
    id=db.Column(db.Integer,primary_key=True)
    user_id=db.Column(db.Integer,db.ForeignKey("user.id"),nullable=False,index=True)
    plan=db.Column(db.String(40),nullable=False,default="free")
    status=db.Column(db.String(30),nullable=False,default="active")
    provider=db.Column(db.String(40),default="")
    provider_customer_id=db.Column(db.String(180),default="")
    provider_subscription_id=db.Column(db.String(180),default="")
    current_period_end=db.Column(db.DateTime)
    created_at=db.Column(db.DateTime,default=lambda:datetime.now(timezone.utc))
    user=db.relationship("User")

class PaymentEvent(db.Model):
    id=db.Column(db.Integer,primary_key=True)
    provider=db.Column(db.String(40),nullable=False)
    event_id=db.Column(db.String(180),nullable=False,unique=True)
    event_type=db.Column(db.String(100),nullable=False)
    status=db.Column(db.String(30),default="received")
    payload=db.Column(db.Text,default="")
    created_at=db.Column(db.DateTime,default=lambda:datetime.now(timezone.utc),index=True)

class OpportunityApplication(db.Model):
    id=db.Column(db.Integer,primary_key=True)
    user_id=db.Column(db.Integer,db.ForeignKey("user.id"),nullable=False)
    opportunity_id=db.Column(db.Integer,db.ForeignKey("opportunity.id"),nullable=False)
    note=db.Column(db.Text,default="")
    created_at=db.Column(db.DateTime,default=lambda:datetime.now(timezone.utc))
    __table_args__=(db.UniqueConstraint("user_id","opportunity_id",name="uq_opportunity_application"),)

@app.before_request
def csrf_and_security():
    session.setdefault("csrf", secrets.token_urlsafe(24))
    # Signed payment webhooks authenticate with the provider signature instead of a user session CSRF token.
    csrf_exempt = request.path == "/billing/webhook"
    if request.method in {"POST","PUT","PATCH","DELETE"} and not csrf_exempt:
        token=request.form.get("csrf_token") or request.headers.get("X-CSRF-Token")
        if not token or not secrets.compare_digest(token, session["csrf"]): abort(400, description="Invalid CSRF token")

@app.after_request
def headers(resp):
    resp.headers["X-Content-Type-Options"]="nosniff"; resp.headers["X-Frame-Options"]="SAMEORIGIN"; resp.headers["Referrer-Policy"]="strict-origin-when-cross-origin"
    resp.headers["Permissions-Policy"]="camera=(self), microphone=(self), geolocation=()"
    resp.headers["Content-Security-Policy"]="default-src 'self'; img-src 'self' data:; media-src 'self' blob:; connect-src 'self' ws: wss:; style-src 'self' 'unsafe-inline'; script-src 'self' 'unsafe-inline'"
    if request.is_secure and os.environ.get("COOKIE_SECURE", "0") == "1":
        resp.headers["Strict-Transport-Security"]="max-age=31536000; includeSubDomains"
    return resp

@app.context_processor
def common():
    uid=session.get("user_id"); user=db.session.get(User,uid) if uid else None
    unread=Notification.query.filter_by(user_id=uid,read=False).count() if uid else 0
    unread_messages=Message.query.filter_by(receiver_id=uid,read=False).count() if uid else 0
    selected=None
    if uid:
        selected=University.query.get(session.get("university_id")) if session.get("university_id") else University.query.filter_by(name=user.university).first()
        if selected and not session.get("university_id"): session["university_id"]=selected.id
    memberships=UniversityMembership.query.filter_by(user_id=uid,status="active").join(University).order_by(University.name.asc()).all() if uid else []
    return {"current_user":user,"csrf_token":session.get("csrf"),"unread_notifications":unread,"unread_messages":unread_messages,"selected_university":selected,"my_universities":memberships}

def login_required(fn):
    @wraps(fn)
    def wrapper(*a,**kw):
        if not session.get("user_id"): return redirect(url_for("login",next=request.path))
        return fn(*a,**kw)
    return wrapper

def notify(user_id,kind,text,url=""):
    n=Notification(user_id=user_id,kind=kind,text=text,url=url)
    db.session.add(n)
    db.session.flush()
    ws_publish(user_id, {"type":"notification","notification": {"id":n.id,"kind":n.kind,"text":n.text,"url":n.url,"read":n.read,"created_at":n.created_at.isoformat()}})

def valid_file(f,allowed):
    if not f or not f.filename: return None
    ext=secure_filename(f.filename).rsplit(".",1)[-1].lower() if "." in f.filename else ""
    if ext not in allowed: return None
    name=f"{uuid.uuid4().hex}.{ext}"
    if os.environ.get("MEDIA_STORAGE", "local").lower() == "s3":
        if not boto3: raise RuntimeError("MEDIA_STORAGE=s3 requires boto3")
        bucket=os.environ.get("S3_BUCKET", "")
        if not bucket: raise RuntimeError("S3_BUCKET is required when MEDIA_STORAGE=s3")
        client=boto3.client("s3", endpoint_url=os.environ.get("S3_ENDPOINT_URL") or None,
            region_name=os.environ.get("S3_REGION") or None,
            aws_access_key_id=os.environ.get("S3_ACCESS_KEY_ID") or None,
            aws_secret_access_key=os.environ.get("S3_SECRET_ACCESS_KEY") or None)
        client.upload_fileobj(f, bucket, f"uploads/{name}", ExtraArgs={"ContentType": f.mimetype or "application/octet-stream"})
        return name
    f.save(os.path.join(UPLOAD,name)); return name

@app.cli.command("init-db")
def init_db():
    db.create_all(); seed(); print("FlexUni database initialized.")

def seed():
    if Community.query.count()==0:
        db.session.add_all([Community(name=n,description=d) for n,d in [
            ("CST Tech Hub","Programming, AI, cybersecurity and tech projects."),("Business & Entrepreneurship","Ideas, startups, marketing and campus businesses."),
            ("Study Together","Find classmates, study groups and exam preparation partners."),("Creative Campus","Design, photography, music, video and creative work.")]])
    if University.query.count()==0:
        db.session.add_all([
            University(name="University of Rwanda",city="Kigali",country="Rwanda",description="Rwanda's public university network.",verified=True,website="https://ur.ac.rw/"),
            University(name="Kigali Independent University",city="Kigali",country="Rwanda",description="A higher education institution in Kigali.",verified=True),
            University(name="Adventist University of Central Africa",city="Kigali",country="Rwanda",description="A regional university in Kigali.",verified=True),
            University(name="University of Kigali",city="Kigali",country="Rwanda",description="A private university in Kigali.",verified=True),
            University(name="Rwanda Polytechnic",city="Kigali",country="Rwanda",description="A public technical and vocational higher education institution.",verified=True)
        ])
    # Backfill an active membership from the legacy User.university field.
    for u in User.query.all():
        uni=University.query.filter_by(name=u.university).first()
        if uni and not UniversityMembership.query.filter_by(user_id=u.id,university_id=uni.id).first():
            db.session.add(UniversityMembership(user_id=u.id,university_id=uni.id,role="student",status="active"))
    db.session.commit()
with app.app_context():
    # Development convenience only. Production deployments should run migrations explicitly.
    if os.environ.get("AUTO_CREATE_DB", "1") == "1":
        db.create_all()
    seed()

@app.get("/")
def index():
    selected_id=session.get("university_id")
    selected=University.query.get(selected_id) if selected_id else None
    query=Post.query.join(User,Post.user_id==User.id)
    if selected: query=query.filter(User.university==selected.name)
    posts=query.order_by(Post.created_at.desc()).limit(50).all()
    products=Product.query.order_by(Product.created_at.desc()).limit(6).all()
    liked=set()
    if session.get("user_id"):
        liked={x.post_id for x in Like.query.filter_by(user_id=session["user_id"]).all()}
    return render_template("index.html",posts=posts,products=products,liked=liked,selected_university=selected)

@app.get("/universities")
def universities():
    q=request.args.get("q","").strip()[:100]
    query=University.query
    if q:
        like=f"%{q}%"; query=query.filter(or_(University.name.ilike(like),University.city.ilike(like),University.country.ilike(like),University.description.ilike(like)))
    rows=[]
    for uni in query.order_by(University.verified.desc(),University.name.asc()).all():
        members=UniversityMembership.query.filter_by(university_id=uni.id,status="active").count()
        rows.append({"obj":uni,"members":members,"joined":bool(session.get("user_id") and UniversityMembership.query.filter_by(user_id=session["user_id"],university_id=uni.id,status="active").first())})
    return render_template("universities.html",universities=rows)

@app.get("/universities/<int:university_id>")
def university_profile(university_id):
    uni=University.query.get_or_404(university_id)
    members=UniversityMembership.query.filter_by(university_id=uni.id,status="active").count()
    posts=Post.query.join(User,Post.user_id==User.id).filter(User.university==uni.name).order_by(Post.created_at.desc()).limit(12).all()
    opportunities=Opportunity.query.join(User,Opportunity.user_id==User.id).filter(User.university==uni.name).order_by(Opportunity.created_at.desc()).limit(8).all()
    services=CampusService.query.filter_by(university_id=uni.id,active=True).order_by(CampusService.created_at.desc()).limit(8).all()
    joined=bool(session.get("user_id") and UniversityMembership.query.filter_by(user_id=session["user_id"],university_id=uni.id,status="active").first())
    return render_template("university.html",university=uni,members=members,posts=posts,opportunities=opportunities,services=services,joined=joined)

@app.post("/universities/<int:university_id>/join")
@login_required
def join_university(university_id):
    uni=University.query.get_or_404(university_id); uid=session["user_id"]
    membership=UniversityMembership.query.filter_by(user_id=uid,university_id=uni.id).first()
    if membership:
        if membership.status!="active": membership.status="active"
        session["university_id"]=uni.id; db.session.commit(); flash(f"You are now connected to {uni.name}.")
    else:
        db.session.add(UniversityMembership(user_id=uid,university_id=uni.id,status="active",role="student"))
        u=User.query.get(uid); u.university=uni.name
        session["university_id"]=uni.id; db.session.commit(); flash(f"You joined {uni.name}.")
    return redirect(url_for("university_profile",university_id=uni.id))

@app.post("/universities/switch")
@login_required
def switch_university():
    uid=request.form.get("university_id",type=int); uni=University.query.get_or_404(uid)
    if not UniversityMembership.query.filter_by(user_id=session["user_id"],university_id=uid,status="active").first(): abort(403)
    session["university_id"]=uid
    return redirect(request.form.get("next") or url_for("index"))

@app.route("/register",methods=["GET","POST"])
def register():
    universities=University.query.order_by(University.name.asc()).all()
    if request.method=="POST":
        name=request.form.get("name","").strip(); email=request.form.get("email","").strip().lower(); password=request.form.get("password",""); course=request.form.get("course","").strip(); university_id=request.form.get("university_id",type=int)
        if len(name)<2 or "@" not in email or len(password)<8: flash("Enter a valid name/email and a password of at least 8 characters."); return redirect(url_for("register"))
        if User.query.filter_by(email=email).first(): flash("That email is already registered."); return redirect(url_for("login"))
        uni=University.query.get(university_id) if university_id else University.query.filter_by(name="University of Rwanda").first()
        u=User(name=name,email=email,password=generate_password_hash(password),course=course,university=uni.name if uni else "University of Rwanda"); db.session.add(u); db.session.flush()
        if uni: db.session.add(UniversityMembership(user_id=u.id,university_id=uni.id,status="active",role="student"))
        db.session.commit(); session.clear(); session["user_id"]=u.id; session["university_id"]=uni.id if uni else None; session["csrf"]=secrets.token_urlsafe(24); return redirect(url_for("index"))
    return render_template("auth.html",mode="register",universities=universities)

@app.route("/login",methods=["GET","POST"])
def login():
    if request.method=="POST":
        u=User.query.filter_by(email=request.form.get("email","").strip().lower()).first()
        if u and check_password_hash(u.password,request.form.get("password","")):
            membership=UniversityMembership.query.filter_by(user_id=u.id,status="active").join(University).order_by(UniversityMembership.joined_at.asc()).first()
            session.clear(); session["user_id"]=u.id; session["university_id"]=membership.university_id if membership else None; session["csrf"]=secrets.token_urlsafe(24); return redirect(request.args.get("next") or url_for("index"))
        flash("Email or password is incorrect.")
    return render_template("auth.html",mode="login")

@app.post("/logout")
@login_required
def logout(): session.clear(); return redirect(url_for("index"))

@app.post("/post")
@login_required
def create_post():
    body=request.form.get("body","").strip(); f=request.files.get("media"); media=valid_file(f,ALLOWED_MEDIA) if f else None
    if not body and not media: flash("Write something or attach media."); return redirect(url_for("index"))
    u=session_user(); selected=University.query.get(session.get("university_id")) if session.get("university_id") else None
    if selected: u.university=selected.name
    p=Post(user_id=session["user_id"],body=body[:5000],media=media,media_type=(f.mimetype if media else "")); db.session.add(p); db.session.commit(); return redirect(url_for("index"))

@app.post("/like/<int:post_id>")
@login_required
def like(post_id):
    p=db.session.get(Post,post_id) or abort(404); existing=Like.query.filter_by(user_id=session["user_id"],post_id=post_id).first()
    if existing: db.session.delete(existing); liked=False
    else:
        db.session.add(Like(user_id=session["user_id"],post_id=post_id)); liked=True
        if p.user_id!=session["user_id"]: notify(p.user_id,"like",f"{session_user().name} liked your post.",url_for("index"))
    db.session.commit(); return jsonify(ok=True,liked=liked,count=Like.query.filter_by(post_id=post_id).count())

def session_user(): return db.session.get(User,session["user_id"])

@app.post("/share/<int:post_id>")
@login_required
def share(post_id):
    p=db.session.get(Post,post_id) or abort(404)
    existing=Share.query.filter_by(user_id=session["user_id"],post_id=post_id).first()
    if existing:
        db.session.delete(existing); shared=False
    else:
        db.session.add(Share(user_id=session["user_id"],post_id=post_id)); shared=True
        if p.user_id!=session["user_id"]: notify(p.user_id,"share",f"{session_user().name} shared your post.",url_for("index"))
    db.session.commit()
    return jsonify(ok=True,shared=shared,count=Share.query.filter_by(post_id=post_id).count())

@app.post("/post/<int:post_id>/comment")
@login_required
def comment(post_id):
    p=db.session.get(Post,post_id) or abort(404); body=request.form.get("body","").strip()
    if body: db.session.add(Comment(user_id=session["user_id"],post_id=post_id,body=body[:1000]));
    if body and p.user_id!=session["user_id"]: notify(p.user_id,"comment",f"{session_user().name} commented on your post.",url_for("index"))
    db.session.commit(); return redirect(url_for("index"))

@app.route("/profile/<int:user_id>",methods=["GET","POST"])
def profile(user_id):
    u=db.session.get(User,user_id) or abort(404)
    if request.method=="POST":
        if session.get("user_id")!=user_id: abort(403)
        u.name=request.form.get("name",u.name).strip()[:120]; u.course=request.form.get("course","").strip()[:180]; u.bio=request.form.get("bio","").strip()[:3000]
        f=request.files.get("photo"); media=valid_file(f,ALLOWED_IMAGES) if f else None
        if media: u.photo=media
        db.session.commit(); flash("Profile updated.")
    return render_template("profile.html",user=u,posts=Post.query.filter_by(user_id=user_id).order_by(Post.created_at.desc()).all())

@app.get("/communities")
def communities():
    uid=session.get("user_id"); rows=[]
    for c in Community.query.order_by(Community.name).all(): rows.append({"obj":c,"members":Membership.query.filter_by(community_id=c.id).count(),"joined":bool(uid and Membership.query.filter_by(user_id=uid,community_id=c.id).first())})
    return render_template("communities.html",communities=rows)

@app.post("/community/<int:cid>/join")
@login_required
def join(cid):
    Community.query.get_or_404(cid); ex=Membership.query.filter_by(user_id=session["user_id"],community_id=cid).first()
    if ex: db.session.delete(ex)
    else: db.session.add(Membership(user_id=session["user_id"],community_id=cid))
    db.session.commit(); return redirect(url_for("communities"))

@app.route("/marketplace",methods=["GET","POST"])
def marketplace():
    if request.method=="POST":
        if not session.get("user_id"): return redirect(url_for("login"))
        try: price=float(request.form.get("price",0))
        except ValueError: price=-1
        if price<0: flash("Enter a valid price."); return redirect(url_for("marketplace"))
        f=request.files.get("image"); image=valid_file(f,ALLOWED_IMAGES) if f else None
        p=Product(seller_id=session["user_id"],title=request.form.get("title","").strip()[:180],price=price,category=request.form.get("category","").strip()[:100],description=request.form.get("description","").strip()[:3000],image=image or ""); db.session.add(p); db.session.commit(); return redirect(url_for("marketplace"))
    return render_template("marketplace.html",products=Product.query.order_by(Product.created_at.desc()).all())

@app.get("/search")
def search():
    q=request.args.get("q","").strip()[:100]; users=[]; posts=[]; products=[]
    if q:
        like=f"%{q}%"; users=User.query.filter(or_(User.name.ilike(like),User.course.ilike(like))).limit(20).all(); posts=Post.query.filter(Post.body.ilike(like)).order_by(Post.created_at.desc()).limit(20).all(); products=Product.query.filter(or_(Product.title.ilike(like),Product.description.ilike(like))).limit(20).all()
    return render_template("search.html",q=q,users=users,posts=posts,products=products)

@app.get("/messages")
@login_required
def messages():
    uid=session["user_id"]
    selected_id=request.args.get("user",type=int)
    selected=db.session.get(User,selected_id) if selected_id else None
    if selected and selected.id==uid:
        selected=None

    # Build a real inbox from the message table: one row per person, ordered by latest activity.
    other_ids=set()
    for m in Message.query.filter(or_(Message.sender_id==uid,Message.receiver_id==uid)).all():
        other_ids.add(m.receiver_id if m.sender_id==uid else m.sender_id)
    contacts=[]
    for cid in other_ids:
        u=db.session.get(User,cid)
        if not u: continue
        last=Message.query.filter(or_((Message.sender_id==uid)&(Message.receiver_id==cid),(Message.sender_id==cid)&(Message.receiver_id==uid))).order_by(Message.created_at.desc()).first()
        unread=Message.query.filter_by(sender_id=cid,receiver_id=uid,read=False).count()
        contacts.append({"user":u,"last":last,"unread":unread})
    contacts.sort(key=lambda x: x["last"].created_at if x["last"] else datetime.min.replace(tzinfo=timezone.utc), reverse=True)

    # Suggested students for starting a new conversation.
    suggestions=User.query.filter(User.id!=uid).order_by(User.name).limit(50).all()
    convo=[]
    if selected:
        convo=Message.query.filter(or_((Message.sender_id==uid)&(Message.receiver_id==selected.id),(Message.sender_id==selected.id)&(Message.receiver_id==uid))).order_by(Message.created_at.asc()).limit(500).all()
        Message.query.filter_by(sender_id=selected.id,receiver_id=uid,read=False).update({"read":True}, synchronize_session=False)
        db.session.commit()
    return render_template("messages.html",contacts=contacts,selected=selected,convo=convo,suggestions=suggestions)

@app.get("/api/messages")
@login_required
def api_messages():
    uid=session["user_id"]; rid=request.args.get("user",type=int)
    if not rid or rid==uid: abort(400, description="Choose another student.")
    User.query.get_or_404(rid)
    before_id=request.args.get("before",type=int)
    q=Message.query.filter(or_((Message.sender_id==uid)&(Message.receiver_id==rid),(Message.sender_id==rid)&(Message.receiver_id==uid)))
    if before_id: q=q.filter(Message.id<before_id)
    rows=q.order_by(Message.id.desc()).limit(80).all()
    rows.reverse()
    Message.query.filter_by(sender_id=rid,receiver_id=uid,read=False).update({"read":True}, synchronize_session=False)
    db.session.commit()
    return jsonify(messages=[{"id":m.id,"sender_id":m.sender_id,"receiver_id":m.receiver_id,"body":m.body,"read":m.read,"created_at":m.created_at.isoformat()} for m in rows])

@app.post("/api/messages/send")
@login_required
def api_send_message():
    uid=session["user_id"]
    if request.is_json:
        payload=request.get_json(silent=True) or {}
        rid=payload.get("receiver_id")
        body=payload.get("body","") or ""
    else:
        rid=request.form.get("receiver_id",type=int)
        body=request.form.get("body","") or ""
    try: rid=int(rid)
    except (TypeError,ValueError): rid=0
    body=body.strip()
    if not rid or rid==uid: abort(400, description="Invalid recipient.")
    recipient=db.session.get(User,rid) or abort(404)
    if not body: abort(400, description="Message cannot be empty.")
    if len(body)>2000: abort(400, description="Message is too long.")
    m=Message(sender_id=uid,receiver_id=rid,body=body)
    db.session.add(m)
    notify(rid,"message",f"{session_user().name} sent you a message.",url_for("messages",user=uid))
    db.session.commit()
    payload={"type":"message","message":{"id":m.id,"sender_id":m.sender_id,"receiver_id":m.receiver_id,"body":m.body,"read":m.read,"created_at":m.created_at.isoformat()}}
    ws_publish_many([uid,rid], payload)
    ws_publish(rid,{"type":"inbox_update","user_id":uid,"peer_id":rid,"unread":Message.query.filter_by(receiver_id=rid,read=False).count()})
    return jsonify(ok=True,message=payload["message"],recipient=recipient.name)

@app.post("/messages/send")
@login_required
def send_message_form():
    rid=request.form.get("receiver_id",type=int); body=request.form.get("body","").strip()
    if not rid or rid==session["user_id"]: abort(400, description="Invalid recipient.")
    db.session.get(User,rid) or abort(404)
    if body:
        db.session.add(Message(sender_id=session["user_id"],receiver_id=rid,body=body[:2000]))
        notify(rid,"message",f"{session_user().name} sent you a message.",url_for("messages",user=session["user_id"]))
        db.session.commit()
    return redirect(url_for("messages",user=rid))

@app.post("/messages/<int:user_id>/read")
@login_required
def mark_messages_read(user_id):
    if user_id==session["user_id"]: abort(400)
    Message.query.filter_by(sender_id=user_id,receiver_id=session["user_id"],read=False).update({"read":True}, synchronize_session=False)
    db.session.commit()
    return jsonify(ok=True)

@app.get("/api/message-inbox")
@login_required
def api_message_inbox():
    uid=session["user_id"]; latest={}; unread={}
    for m in Message.query.filter(or_(Message.sender_id==uid,Message.receiver_id==uid)).order_by(Message.created_at.desc()).all():
        other=m.receiver_id if m.sender_id==uid else m.sender_id
        if other not in latest: latest[other]=m
    for row in Message.query.filter_by(receiver_id=uid,read=False).all(): unread[row.sender_id]=unread.get(row.sender_id,0)+1
    data=[]
    for oid,m in latest.items():
        u=db.session.get(User,oid)
        if u: data.append({"user_id":oid,"name":u.name,"photo":u.photo or "","last_message":m.body,"created_at":m.created_at.isoformat(),"unread":unread.get(oid,0)})
    data.sort(key=lambda x:x["created_at"],reverse=True)
    return jsonify(conversations=data)

@app.get("/api/notifications")
@login_required
def api_notifications():
    uid=session["user_id"]; limit=min(request.args.get("limit",50,type=int) or 50,100)
    rows=Notification.query.filter_by(user_id=uid).order_by(Notification.created_at.desc()).limit(limit).all()
    return jsonify(notifications=[{"id":n.id,"kind":n.kind,"text":n.text,"url":n.url,"read":n.read,"created_at":n.created_at.isoformat()} for n in rows],unread=Notification.query.filter_by(user_id=uid,read=False).count())

@app.post("/notifications/<int:notification_id>/read")
@login_required
def mark_notification_read(notification_id):
    n=Notification.query.filter_by(id=notification_id,user_id=session["user_id"]).first() or abort(404)
    n.read=True; db.session.commit()
    ws_publish(session["user_id"], {"type":"unread_counts","notifications":Notification.query.filter_by(user_id=session["user_id"],read=False).count(),"messages":Message.query.filter_by(receiver_id=session["user_id"],read=False).count()})
    return jsonify(ok=True)

@app.post("/notifications/read-all")
@login_required
def mark_all_notifications_read():
    Notification.query.filter_by(user_id=session["user_id"],read=False).update({"read":True}, synchronize_session=False)
    db.session.commit()
    ws_publish(session["user_id"], {"type":"unread_counts","notifications":0,"messages":Message.query.filter_by(receiver_id=session["user_id"],read=False).count()})
    return jsonify(ok=True)

@app.get("/api/unread-counts")
@login_required
def unread_counts():
    uid=session["user_id"]
    return jsonify(notifications=Notification.query.filter_by(user_id=uid,read=False).count(),messages=Message.query.filter_by(receiver_id=uid,read=False).count())

@app.get("/notifications")
@login_required
def notifications():
    items=Notification.query.filter_by(user_id=session["user_id"]).order_by(Notification.created_at.desc()).limit(100).all()
    return render_template("notifications.html",items=items)


@sock.route("/ws")
def realtime_socket(ws):
    user_id=session.get("user_id")
    if not user_id:
        try: ws.close()
        except Exception: pass
        return
    ws_register(user_id, ws)
    try:
        ws.send(json.dumps({"type":"ready","user_id":user_id}))
        while True:
            raw=ws.receive()
            if raw is None:
                break
            try:
                data=json.loads(raw)
            except Exception:
                continue
            kind=data.get("type")
            if kind=="ping":
                ws.send(json.dumps({"type":"pong"}))
            elif kind=="typing":
                peer_id=int(data.get("to",0) or 0)
                if peer_id and peer_id!=user_id:
                    ws_publish(peer_id,{"type":"typing","from":user_id,"active":bool(data.get("active"))})
    finally:
        ws_unregister(user_id, ws)


def admin_required(fn):
    @wraps(fn)
    @login_required
    def wrapper(*a,**kw):
        if session_user().role != "admin": abort(403)
        return fn(*a,**kw)
    return wrapper

REPORTABLE={"post":"Post","comment":"Comment","product":"Product","resource":"Resource","opportunity":"Opportunity","user":"User"}

def target_exists(target_type,target_id):
    model={"post":Post,"comment":Comment,"product":Product,"resource":Resource,"opportunity":Opportunity,"user":User}.get(target_type)
    return model and db.session.get(model,target_id)

@app.post("/reports")
@login_required
def create_report():
    target_type=request.form.get("target_type","").strip().lower(); target_id=request.form.get("target_id",type=int)
    reason=request.form.get("reason","Other").strip()[:120]; details=request.form.get("details","").strip()[:2000]
    if target_type not in REPORTABLE or not target_id or not target_exists(target_type,target_id): abort(404)
    if target_type=="user" and target_id==session["user_id"]: abort(400)
    existing=Report.query.filter_by(reporter_id=session["user_id"],target_type=target_type,target_id=target_id,status="open").first()
    if not existing:
        db.session.add(Report(reporter_id=session["user_id"],target_type=target_type,target_id=target_id,reason=reason or "Other",details=details))
        db.session.commit(); flash("Thanks. Your report was sent to FlexUni moderators.")
    else: flash("You already reported this item; moderators will review it.")
    return redirect(request.referrer or url_for("index"))

@app.get("/admin")
@admin_required
def admin_dashboard():
    reports=Report.query.filter_by(status="open").order_by(Report.created_at.desc()).limit(100).all()
    recent=ModerationAction.query.order_by(ModerationAction.created_at.desc()).limit(30).all()
    stats={"users":User.query.count(),"posts":Post.query.count(),"reports":Report.query.filter_by(status="open").count(),"products":Product.query.count(),"resources":Resource.query.count(),"opportunities":Opportunity.query.count()}
    return render_template("admin.html",reports=reports,recent=recent,stats=stats)

@app.get("/admin/users")
@admin_required
def admin_users():
    q=request.args.get("q","").strip()[:100]
    query=User.query.order_by(User.created_at.desc())
    if q:
        like=f"%{q}%"; query=query.filter(or_(User.name.ilike(like),User.email.ilike(like),User.university.ilike(like)))
    users=query.limit(200).all()
    statuses={m.user_id:m for m in UserModeration.query.filter(UserModeration.user_id.in_([u.id for u in users])).all()} if users else {}
    return render_template("admin_users.html",users=users,statuses=statuses,q=q)

@app.post("/admin/reports/<int:report_id>/resolve")
@admin_required
def resolve_report(report_id):
    r=Report.query.get_or_404(report_id); action=request.form.get("action","dismiss").strip(); note=request.form.get("note","").strip()[:1000]
    if r.status!="open": return redirect(url_for("admin_dashboard"))
    r.status="resolved" if action in {"remove","warn","suspend","ban","dismiss"} else "dismissed"
    r.reviewed_by=session["user_id"]; r.reviewed_at=datetime.now(timezone.utc)
    db.session.add(ModerationAction(admin_id=session["user_id"],action=action,target_type=r.target_type,target_id=r.target_id,note=note))
    if action=="remove":
        obj=target_exists(r.target_type,r.target_id)
        if obj and r.target_type!="user": db.session.delete(obj)
    elif action in {"suspend","ban"} and r.target_type=="user":
        m=UserModeration.query.filter_by(user_id=r.target_id).first() or UserModeration(user_id=r.target_id)
        m.status=action; m.reason=note or r.reason
        if action=="suspend": m.expires_at=datetime.now(timezone.utc).replace(microsecond=0)+__import__('datetime').timedelta(days=7)
        else: m.expires_at=None
        db.session.add(m)
    db.session.commit(); flash("Report action recorded."); return redirect(url_for("admin_dashboard"))

@app.post("/admin/users/<int:user_id>/moderate")
@admin_required
def moderate_user(user_id):
    u=User.query.get_or_404(user_id); action=request.form.get("action","warn"); note=request.form.get("note","").strip()[:1000]
    if u.id==session["user_id"]: abort(400)
    if action=="role_admin": u.role="admin"
    elif action=="role_student": u.role="student"
    elif action in {"suspend","ban","unsuspend"}:
        m=UserModeration.query.filter_by(user_id=u.id).first() or UserModeration(user_id=u.id)
        m.status="active" if action=="unsuspend" else action; m.reason=note
        m.expires_at=(datetime.now(timezone.utc)+__import__('datetime').timedelta(days=7)) if action=="suspend" else None
        db.session.add(m)
    db.session.add(ModerationAction(admin_id=session["user_id"],action=action,target_type="user",target_id=u.id,note=note))
    db.session.commit(); flash("User moderation action saved."); return redirect(url_for("admin_users"))

@app.post("/admin/content/<target_type>/<int:target_id>/remove")
@admin_required
def admin_remove_content(target_type,target_id):
    if target_type not in {"post","comment","product","resource","opportunity"}: abort(404)
    obj=target_exists(target_type,target_id) or abort(404); db.session.delete(obj)
    db.session.add(ModerationAction(admin_id=session["user_id"],action="remove",target_type=target_type,target_id=target_id,note=request.form.get("note","")[:1000]))
    db.session.commit(); flash("Content removed."); return redirect(request.referrer or url_for("admin_dashboard"))

@app.route("/study", methods=["GET","POST"])
@login_required
def study():
    if request.method == "POST":
        title=request.form.get("title","").strip()[:180]
        description=request.form.get("description","").strip()[:5000]
        link=request.form.get("link","").strip()[:500]
        category=request.form.get("category","Study").strip()[:80] or "Study"
        if not title or not description:
            flash("Add a title and a short description for the resource.")
            return redirect(url_for("study"))
        db.session.add(Resource(user_id=session["user_id"],title=title,description=description,link=link,category=category))
        db.session.commit(); flash("Study resource published.")
        return redirect(url_for("study"))
    q=request.args.get("q","").strip()[:100]; category=request.args.get("category","").strip()[:80]
    query=Resource.query
    if q:
        like=f"%{q}%"; query=query.filter(or_(Resource.title.ilike(like),Resource.description.ilike(like),Resource.category.ilike(like)))
    if category: query=query.filter(Resource.category==category)
    resources=query.order_by(Resource.created_at.desc()).all()
    bookmarks={x.resource_id for x in ResourceBookmark.query.filter_by(user_id=session["user_id"]).all()}
    categories=[x[0] for x in db.session.query(Resource.category).distinct().order_by(Resource.category).all() if x[0]]
    return render_template("study.html",resources=resources,bookmarks=bookmarks,q=q,category=category,categories=categories)

@app.post("/study/<int:resource_id>/bookmark")
@login_required
def bookmark_resource(resource_id):
    Resource.query.get_or_404(resource_id)
    ex=ResourceBookmark.query.filter_by(user_id=session["user_id"],resource_id=resource_id).first()
    if ex: db.session.delete(ex); saved=False
    else: db.session.add(ResourceBookmark(user_id=session["user_id"],resource_id=resource_id)); saved=True
    db.session.commit(); return jsonify(ok=True,saved=saved)

@app.post("/study/<int:resource_id>/delete")
@login_required
def delete_resource(resource_id):
    r=Resource.query.get_or_404(resource_id)
    if r.user_id!=session["user_id"] and session_user().role!="admin": abort(403)
    db.session.delete(r); db.session.commit(); flash("Resource removed."); return redirect(url_for("study"))

@app.route("/opportunities", methods=["GET","POST"])
@login_required
def opportunities():
    if request.method == "POST":
        title=request.form.get("title","").strip()[:180]
        description=request.form.get("description","").strip()[:5000]
        link=request.form.get("link","").strip()[:500]
        category=request.form.get("category","Opportunity").strip()[:80] or "Opportunity"
        raw_deadline=request.form.get("deadline","").strip()
        deadline=None
        if raw_deadline:
            try: deadline=datetime.fromisoformat(raw_deadline).replace(tzinfo=timezone.utc)
            except ValueError: flash("Use a valid deadline date and time."); return redirect(url_for("opportunities"))
        if not title or not description:
            flash("Add a title and a description for the opportunity.")
            return redirect(url_for("opportunities"))
        db.session.add(Opportunity(user_id=session["user_id"],title=title,description=description,link=link,category=category,deadline=deadline))
        db.session.commit(); flash("Opportunity published.")
        return redirect(url_for("opportunities"))
    q=request.args.get("q","").strip()[:100]; category=request.args.get("category","").strip()[:80]; status=request.args.get("status","open").strip()
    query=Opportunity.query
    now=datetime.now(timezone.utc)
    if q:
        like=f"%{q}%"; query=query.filter(or_(Opportunity.title.ilike(like),Opportunity.description.ilike(like),Opportunity.category.ilike(like)))
    if category: query=query.filter(Opportunity.category==category)
    if status=="open": query=query.filter(or_(Opportunity.deadline==None,Opportunity.deadline>=now))
    elif status=="closed": query=query.filter(Opportunity.deadline!=None,Opportunity.deadline<now)
    opps=query.order_by(Opportunity.created_at.desc()).all()
    applied={x.opportunity_id for x in OpportunityApplication.query.filter_by(user_id=session["user_id"]).all()}
    categories=[x[0] for x in db.session.query(Opportunity.category).distinct().order_by(Opportunity.category).all() if x[0]]
    return render_template("opportunities.html",opportunities=opps,applied=applied,q=q,category=category,status=status,categories=categories,now=now)

@app.post("/opportunities/<int:opportunity_id>/apply")
@login_required
def apply_opportunity(opportunity_id):
    opp=Opportunity.query.get_or_404(opportunity_id)
    if opp.deadline and opp.deadline < datetime.now(timezone.utc):
        flash("This opportunity has closed."); return redirect(url_for("opportunities"))
    if opp.user_id==session["user_id"]:
        flash("You cannot apply to your own opportunity."); return redirect(url_for("opportunities"))
    if not OpportunityApplication.query.filter_by(user_id=session["user_id"],opportunity_id=opportunity_id).first():
        note=request.form.get("note","").strip()[:2000]
        db.session.add(OpportunityApplication(user_id=session["user_id"],opportunity_id=opportunity_id,note=note))
        if opp.user_id!=session["user_id"]: notify(opp.user_id,"application",f"{session_user().name} applied to your opportunity.",url_for("opportunities"))
        db.session.commit(); flash("Application recorded. Follow the opportunity link for the official application process.")
    else: flash("You already applied to this opportunity.")
    return redirect(url_for("opportunities"))

@app.post("/opportunities/<int:opportunity_id>/delete")
@login_required
def delete_opportunity(opportunity_id):
    o=Opportunity.query.get_or_404(opportunity_id)
    if o.user_id!=session["user_id"] and session_user().role!="admin": abort(403)
    db.session.delete(o); db.session.commit(); flash("Opportunity removed."); return redirect(url_for("opportunities"))


def ai_generate(prompt, conversation_id=None):
    api_key=os.environ.get("AI_API_KEY", "").strip()
    base_url=os.environ.get("AI_BASE_URL", "").rstrip("/")
    model=os.environ.get("AI_MODEL", "").strip()
    if not (api_key and base_url and model):
        return ("I’m your FlexUni Study Assistant. Ask me to explain a concept, create revision questions, "
                "organize study notes, or make a study plan. Configure AI_API_KEY, AI_BASE_URL and AI_MODEL "
                "to enable the live AI provider.")
    payload=json.dumps({"model":model,"messages":[{"role":"system","content":"You are FlexUni's student study assistant. Be concise, educational, age-appropriate, and never provide unsafe instructions."},{"role":"user","content":prompt}],"temperature":0.4,"max_tokens":800}).encode()
    req=urllib.request.Request(base_url + "/chat/completions", data=payload, headers={"Content-Type":"application/json","Authorization":f"Bearer {api_key}"}, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            data=json.loads(resp.read().decode("utf-8"))
        return data["choices"][0]["message"]["content"].strip()[:12000]
    except (urllib.error.URLError, urllib.error.HTTPError, KeyError, IndexError, json.JSONDecodeError, TimeoutError):
        return "The AI service is temporarily unavailable. Your message was saved; please try again shortly."

@app.route("/ai", methods=["GET","POST"])
@login_required
def ai_assistant():
    conv=AIConversation.query.filter_by(user_id=session["user_id"]).order_by(AIConversation.created_at.desc()).first()
    if request.method=="POST":
        prompt=request.form.get("message","").strip()[:4000]
        if not prompt: return redirect(url_for("ai_assistant"))
        if not conv:
            conv=AIConversation(user_id=session["user_id"]); db.session.add(conv); db.session.flush()
        db.session.add(AIMessage(conversation_id=conv.id,role="user",body=prompt))
        answer = ai_generate(prompt, conv.id)
        db.session.add(AIMessage(conversation_id=conv.id,role="assistant",body=answer)); db.session.commit()
        return redirect(url_for("ai_assistant"))
    messages=AIMessage.query.filter_by(conversation_id=conv.id).order_by(AIMessage.created_at.asc()).all() if conv else []
    return render_template("ai.html",conversation=conv,messages=messages)

@app.get("/api/rtc-config")
@login_required
def rtc_config():
    raw=os.environ.get("RTC_ICE_SERVERS_JSON", "[{\"urls\":[\"stun:stun.l.google.com:19302\"]}]")
    try:
        servers=json.loads(raw)
        if not isinstance(servers,list): raise ValueError
    except Exception:
        servers=[]
    return jsonify(iceServers=servers, provider=os.environ.get("RTC_PROVIDER", "webrtc"))

@app.get("/video")
@login_required
def video_rooms():
    rooms=VideoRoom.query.filter_by(active=True).order_by(VideoRoom.created_at.desc()).limit(50).all()
    universities=University.query.order_by(University.name.asc()).all()
    return render_template("video.html",rooms=rooms,universities=universities)

@app.post("/video/create")
@login_required
def create_video_room():
    title=request.form.get("title","").strip()[:180] or "FlexUni Study Call"
    uid=request.form.get("university_id",type=int)
    room=VideoRoom(id=str(uuid.uuid4()),host_id=session["user_id"],title=title,university_id=uid or None)
    db.session.add(room); db.session.commit()
    return redirect(url_for("video_room",room_id=room.id))

@app.get("/video/<room_id>")
@login_required
def video_room(room_id):
    room=VideoRoom.query.get_or_404(room_id)
    return render_template("video_room.html",room=room)

@app.post("/video/<room_id>/end")
@login_required
def end_video_room(room_id):
    room=VideoRoom.query.get_or_404(room_id)
    if room.host_id!=session["user_id"] and User.query.get(session["user_id"]).role!="admin": abort(403)
    room.active=False; db.session.commit(); return redirect(url_for("video_rooms"))

@app.get("/live")
@login_required
def live_streams():
    streams=LiveStream.query.filter_by(active=True).order_by(LiveStream.created_at.desc()).limit(50).all()
    universities=University.query.order_by(University.name.asc()).all()
    return render_template("live.html",streams=streams,universities=universities)

@app.post("/live/create")
@login_required
def create_live_stream():
    title=request.form.get("title","").strip()[:180] or "FlexUni Live"
    stream=LiveStream(id=str(uuid.uuid4()),host_id=session["user_id"],title=title,description=request.form.get("description","").strip()[:3000],university_id=request.form.get("university_id",type=int) or None)
    db.session.add(stream); db.session.commit(); return redirect(url_for("live_stream",stream_id=stream.id))

@app.get("/live/<stream_id>")
@login_required
def live_stream(stream_id):
    stream=LiveStream.query.get_or_404(stream_id)
    chat=LiveMessage.query.filter_by(stream_id=stream.id).order_by(LiveMessage.created_at.desc()).limit(100).all()[::-1]
    return render_template("live_room.html",stream=stream,chat=chat)

@app.post("/live/<stream_id>/message")
@login_required
def live_message(stream_id):
    stream=LiveStream.query.get_or_404(stream_id)
    body=request.form.get("body","").strip()[:500]
    if body:
        db.session.add(LiveMessage(stream_id=stream.id,user_id=session["user_id"],body=body)); db.session.commit()
    return redirect(url_for("live_stream",stream_id=stream.id))

@app.post("/live/<stream_id>/end")
@login_required
def end_live_stream(stream_id):
    stream=LiveStream.query.get_or_404(stream_id)
    if stream.host_id!=session["user_id"] and User.query.get(session["user_id"]).role!="admin": abort(403)
    stream.active=False; db.session.commit(); return redirect(url_for("live_streams"))

@app.get("/api/live/<stream_id>/chat")
@login_required
def live_chat_api(stream_id):
    rows=LiveMessage.query.filter_by(stream_id=stream_id).order_by(LiveMessage.created_at.desc()).limit(100).all()
    return jsonify(messages=[{"id":x.id,"user":User.query.get(x.user_id).name,"body":x.body,"created_at":x.created_at.isoformat()} for x in rows[::-1]])

@app.get("/healthz")
def healthz():
    try:
        db.session.execute(text("SELECT 1"))
        r=redis_client()
        redis_status="ok" if r else ("disabled" if not os.environ.get("REDIS_URL") else "error")
        return jsonify(status="ok" if redis_status != "error" else "degraded", database="ok", redis=redis_status, media_storage=os.environ.get("MEDIA_STORAGE", "local"))
    except Exception:
        return jsonify(status="degraded", database="error"), 503

@app.get("/readyz")
def readyz():
    try:
        db.session.execute(text("SELECT 1"))
        if os.environ.get("REDIS_REQUIRED", "0") == "1" and not redis_client():
            return jsonify(status="not_ready", redis="error"), 503
        return jsonify(status="ready")
    except Exception:
        return jsonify(status="not_ready"), 503

@app.get("/plans")
def plans():
    current=None
    if session.get("user_id"):
        current=Subscription.query.filter_by(user_id=session["user_id"]).order_by(Subscription.created_at.desc()).first()
    return render_template("plans.html", current=current)

@app.post("/billing/checkout")
@login_required
def billing_checkout():
    plan=request.form.get("plan", "").strip().lower()
    if plan not in {"student_plus", "business"}:
        flash("Choose a valid plan.")
        return redirect(url_for("plans"))
    # Provider-neutral checkout placeholder: no money is collected here.
    sub=Subscription(user_id=session["user_id"], plan=plan, status="pending", provider=os.environ.get("PAYMENT_PROVIDER", ""))
    db.session.add(sub); db.session.commit()
    flash("Checkout is ready for your configured payment provider. No payment was charged by FlexUni in this demo environment.")
    return redirect(url_for("plans"))

@app.route("/business", methods=["GET", "POST"])
@login_required
def business_account():
    account=BusinessAccount.query.filter_by(owner_id=session["user_id"]).first()
    if request.method=="POST":
        name=request.form.get("name", "").strip()
        if len(name)<2:
            flash("Business name is required."); return redirect(url_for("business_account"))
        if not account:
            account=BusinessAccount(owner_id=session["user_id"], name=name)
            db.session.add(account)
        account.name=name[:180]; account.description=request.form.get("description", "").strip()[:5000]; account.website=request.form.get("website", "").strip()[:500]
        db.session.commit(); flash("Business profile saved.")
        return redirect(url_for("business_account"))
    return render_template("business.html", account=account)

@app.get("/campus")
def campus():
    q=request.args.get("q","").strip()[:100]
    category=request.args.get("category","").strip()[:100]
    university_id=request.args.get("university_id",type=int)
    query=CampusService.query.filter_by(active=True)
    if q:
        like=f"%{q}%"
        query=query.filter(or_(CampusService.name.ilike(like),CampusService.description.ilike(like),CampusService.category.ilike(like),CampusService.location.ilike(like)))
    if category: query=query.filter_by(category=category)
    if university_id: query=query.filter_by(university_id=university_id)
    services=query.order_by(CampusService.created_at.desc()).all()
    categories=[x[0] for x in db.session.query(CampusService.category).filter(CampusService.active.is_(True),CampusService.category!="").distinct().order_by(CampusService.category).all()]
    universities=University.query.order_by(University.name.asc()).all()
    businesses=BusinessAccount.query.filter_by(verified=True).order_by(BusinessAccount.name.asc()).limit(24).all()
    return render_template("campus.html",services=services,categories=categories,universities=universities,businesses=businesses)

@app.route("/business/services",methods=["GET","POST"])
@login_required
def business_services():
    account=BusinessAccount.query.filter_by(owner_id=session["user_id"]).first()
    if not account:
        flash("Create your business profile first."); return redirect(url_for("business_account"))
    if request.method=="POST":
        name=request.form.get("name","").strip()[:180]
        if len(name)<2:
            flash("Service name is required."); return redirect(url_for("business_services"))
        uid=request.form.get("university_id",type=int)
        service=CampusService(business_id=account.id,university_id=uid or None,name=name,
            category=request.form.get("category","General").strip()[:100],
            description=request.form.get("description","").strip()[:5000],
            price_label=request.form.get("price_label","").strip()[:100],
            location=request.form.get("location","").strip()[:240],
            phone=request.form.get("phone","").strip()[:50],
            contact_url=request.form.get("contact_url","").strip()[:500])
        db.session.add(service); db.session.commit()
        flash("Campus service published."); return redirect(url_for("business_services"))
    services=CampusService.query.filter_by(business_id=account.id).order_by(CampusService.created_at.desc()).all()
    return render_template("business_services.html",account=account,services=services,universities=University.query.order_by(University.name.asc()).all())

@app.post("/business/services/<int:sid>/toggle")
@login_required
def toggle_service(sid):
    account=BusinessAccount.query.filter_by(owner_id=session["user_id"]).first()
    service=CampusService.query.get_or_404(sid)
    if not account or service.business_id!=account.id: abort(403)
    service.active=not service.active; db.session.commit()
    return redirect(url_for("business_services"))

@app.post("/business/services/<int:sid>/delete")
@login_required
def delete_service(sid):
    account=BusinessAccount.query.filter_by(owner_id=session["user_id"]).first()
    service=CampusService.query.get_or_404(sid)
    if not account or service.business_id!=account.id: abort(403)
    db.session.delete(service); db.session.commit(); flash("Service removed.")
    return redirect(url_for("business_services"))

@app.post("/admin/business/<int:bid>/verify")
@login_required
def verify_business(bid):
    if User.query.get(session["user_id"]).role!="admin": abort(403)
    account=BusinessAccount.query.get_or_404(bid)
    account.verified=not account.verified
    db.session.commit()
    flash("Business verification updated.")
    return redirect(request.referrer or url_for("admin_dashboard"))

@app.post("/billing/webhook")
def billing_webhook():
    provider=request.headers.get("X-Payment-Provider", "unknown")
    event_id=request.headers.get("X-Event-Id", "")
    event_type=request.headers.get("X-Event-Type", "unknown")
    signature=request.headers.get("X-Webhook-Signature", "")
    secret=os.environ.get("PAYMENT_WEBHOOK_SECRET", "")
    raw=request.get_data()
    if not event_id: return jsonify(error="missing event id"), 400
    if not secret: return jsonify(error="webhook verification is not configured"), 503
    expected=hmac.new(secret.encode(), raw, hashlib.sha256).hexdigest()
    if not signature or not hmac.compare_digest(signature, expected): return jsonify(error="invalid signature"), 401
    if PaymentEvent.query.filter_by(event_id=event_id).first(): return jsonify(ok=True, duplicate=True)
    db.session.add(PaymentEvent(provider=provider,event_id=event_id,event_type=event_type,payload=raw[:50000].decode("utf-8", "replace")))
    db.session.commit()
    return jsonify(ok=True), 202

@app.errorhandler(400)
def bad(e): return render_template("error.html",code=400,message=e.description or "Bad request"),400
@app.errorhandler(403)
def forbidden(e): return render_template("error.html",code=403,message="You do not have permission to do that."),403
@app.errorhandler(404)
def missing(e): return render_template("error.html",code=404,message="Page not found."),404

if __name__=="__main__": app.run(host="127.0.0.1",port=int(os.environ.get("PORT",5000)),debug=os.environ.get("FLASK_DEBUG","0")=="1")
