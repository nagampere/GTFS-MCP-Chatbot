import streamlit as st

def show_loading_animation(loading) -> None:
    loading.markdown(
        """
        <style>
            .gtfs-loading-wrap{padding:1.5rem 0;}
            .gtfs-loading-scene{position:relative;width:100%;height:4rem;overflow:hidden;}

            .gtfs-runner{position:absolute;bottom:1.25rem;font-size:2rem;animation:gtfsDogRun 10s linear infinite;}
            @keyframes gtfsDogRun{0%{left:calc(100% + 3rem);}100%{left:-3rem;}}

            .gtfs-dog{display:inline-block;}
            .gtfs-trail{position:absolute;left:2.2rem;bottom:0.05rem;font-size:0.9rem;opacity:0.6;white-space:nowrap;}
            .gtfs-trail span{display:inline-block;animation:gtfsPawPulse 0.6s steps(2,end) infinite;}
            @keyframes gtfsPawPulse{0%{opacity:0.25;transform:translateY(0);}50%{opacity:0.75;transform:translateY(-1px);}100%{opacity:0.25;transform:translateY(0);}}

            .gtfs-loading-caption{text-align:center;opacity:0.75;font-size:0.9rem;margin-top:0.5rem;}
        </style>
        <div class="gtfs-loading-wrap">
            <div class="gtfs-loading-scene">
            <div class="gtfs-runner">
              <span class="gtfs-dog">🐕</span>
              <span class="gtfs-trail"><span>🐾 🐾 🐾</span></span>
            </div>
            </div>
            <div class="gtfs-loading-caption">回答を生成中…</div>
        </div>
        """,
        unsafe_allow_html=True,
    )