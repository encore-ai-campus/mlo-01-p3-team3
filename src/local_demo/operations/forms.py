from urllib.parse import urlsplit
from django import forms
from sketch.catalog import CATEGORIES
from .models import AdCampaign

SLOTS={'editor-right':'그림판 오른쪽','result-right':'결과 오른쪽','my-right':'My 오른쪽'}


class CampaignForm(forms.ModelForm):
    revision=forms.IntegerField(widget=forms.HiddenInput,initial=0)
    reason=forms.CharField(label='변경 사유',max_length=500,min_length=2)
    category=forms.ChoiceField(label='품목',choices=[(k,v) for k,v in CATEGORIES.items() if k!='other'])
    slot_id=forms.ChoiceField(label='광고 위치',choices=SLOTS.items())
    class Meta:
        model=AdCampaign
        fields=['campaign_id','title','description','advertiser','creative','landing_url','category','slot_id','bid_units','starts_at','ends_at','active']
        labels={'campaign_id':'캠페인 ID','title':'광고 제목','description':'광고 문구','advertiser':'광고주명(데모)',
          'creative':'배너 이미지','landing_url':'연결 주소','bid_units':'가상 입찰 포인트','starts_at':'시작일시(KST)','ends_at':'종료일시(KST)','active':'집행 요청 활성'}
        widgets={'creative':forms.FileInput(), 'starts_at':forms.DateTimeInput(format='%Y-%m-%dT%H:%M',attrs={'type':'datetime-local'}),
                 'ends_at':forms.DateTimeInput(format='%Y-%m-%dT%H:%M',attrs={'type':'datetime-local'}),
                 'description':forms.Textarea(attrs={'rows':3})}

    def clean_landing_url(self):
        url=self.cleaned_data['landing_url']
        if url:
            parsed=urlsplit(url)
            if parsed.scheme!='https' or not parsed.hostname or parsed.username or parsed.password:raise forms.ValidationError('HTTPS 판매/안내 주소를 입력해 주세요.')
        return url

    def clean_bid_units(self):
        value=self.cleaned_data['bid_units']
        if not 1<=value<=10000:raise forms.ValidationError('가상 포인트는 1~10000으로 입력해 주세요.')
        return value

    def clean_creative(self):
        image=self.cleaned_data.get('creative')
        if image and getattr(image,'size',0)>10*1024*1024:raise forms.ValidationError('10MB 이하의 배너 이미지를 선택해 주세요.')
        if image and hasattr(image,'image') and image.image.format not in ('JPEG','PNG','WEBP'):raise forms.ValidationError('JPG·PNG·WebP 이미지를 선택해 주세요.')
        return image

    def clean(self):
        data=super().clean()
        if data.get('starts_at') and data.get('ends_at') and data['starts_at']>=data['ends_at']:raise forms.ValidationError('종료일시는 시작일시보다 뒤여야 합니다.')
        return data
