from django.db import models

# properties model starts here 

class Location(models.Model):
    LOCATION_TYPES = [
        ('country', 'Country'),
        ('state', 'State'),
        ('city', 'City'),
    ]
    name = models.CharField(max_length=100)
    type = models.CharField(max_length=20, choices=LOCATION_TYPES)
    latitude = models.FloatField(null=True, blank=True)
    longitude = models.FloatField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)  
    updated_at = models.DateTimeField(auto_now=True)     

    class Meta:
        unique_together = ['name', 'type']
        verbose_name_plural = "Locations"

    def __str__(self):
        return f"{self.get_type_display()}: {self.name}"

class Amenity(models.Model):
    name = models.CharField(max_length=100, unique=True)
    created_at = models.DateTimeField(auto_now_add=True)  
    updated_at = models.DateTimeField(auto_now=True)      

    class Meta:
        verbose_name_plural = "Amenities"

    def __str__(self):
        return self.name

class Property(models.Model):
    title = models.CharField(max_length=200)
    description = models.TextField(null=True, blank=True)
    locations = models.ManyToManyField(Location)
    amenities = models.ManyToManyField(Amenity)
    created_at = models.DateTimeField(auto_now_add=True)  
    updated_at = models.DateTimeField(auto_now=True)      

    class Meta:
        verbose_name_plural = "Properties"

    def __str__(self):
        return f"{self.title}"

class Image(models.Model):
    property = models.ForeignKey(Property, on_delete=models.CASCADE, related_name='images')
    image = models.ImageField(upload_to='property_images/')
    created_at = models.DateTimeField(auto_now_add=True)  
    updated_at = models.DateTimeField(auto_now=True)      

    def __str__(self):
        return f"Image for {self.property.title}"
